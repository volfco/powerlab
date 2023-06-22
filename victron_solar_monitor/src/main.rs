use anyhow::{bail, Context};
use clap::Parser;
use log::{debug, error, info, trace};
use rumqttc::{Client, MqttOptions, QoS};
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::thread;
use std::time::Duration;
use vedirect::{Events, VEError, MPPT};

/// Read data from a Victron MPPT solar charge controller via a VE.Direct serial connection and publish it to an MQTT broker.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[clap(short, long)]
    device: String,

    /// MQTT broker address
    #[clap(short, long)]
    host: String,

    /// MQTT Broker port
    #[clap(short, long)]
    port: u16,

    /// MQTT topic
    #[clap(short, long)]
    topic: String,
}

struct Listener {
    sink_chan: mpsc::Sender<MPPT>,
}
impl Events<MPPT> for Listener {
    fn on_complete_block(&mut self, block: MPPT) {
        trace!("read data {:?}", &block);
        if let Err(err) = self.sink_chan.send(block) {
            error!("unable to send block to channel. {:?}", err);
        }
    }

    fn on_missing_field(&mut self, label: String) {
        error!("missing field: {:?}", label);
    }

    fn on_mapping_error(&mut self, error: VEError) {
        error!("mapping error: {:?}", error);
    }

    fn on_parse_error(&mut self, error: VEError, _parse_buf: &[u8]) {
        error!("parse error: {:?}", error)
    }
}

fn mqtt_publisher(topic: String, mut client: Client, rx: Receiver<MPPT>) {
    trace!("mqtt publisher thread started");
    let mut count = 0;
    loop {
        let block = rx.recv().unwrap();
        let payload = serde_json::to_string(&block).unwrap();
        trace!("published to topic '{}'. payload : {:?}", &topic, &payload);

        count += 1;
        if count % 10 == 0 {
            info!("{}", &payload);
            count = 0;
        }

        if let Err(e) = client.publish(&topic, QoS::AtLeastOnce, false, payload) {
            error!("unable to publish to topic '{}'. {:?}", &topic, e);
        }
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    debug!("parsed args: {:#?}", &args);

    // open serial port
    debug!("opening serial port: {}", args.device);

    let mut port = serialport::new(args.device.clone(), 19_200)
        .data_bits(serialport::DataBits::Eight)
        .timeout(core::time::Duration::from_secs(2))
        .open()
        .context("Failed to open vedirect serial port")?;

    info!("successfully opened serial port: {}", args.device);

    // connect to mqtt broker
    let (mut client, mut connection) =
        Client::new(MqttOptions::new("victron", &args.host, args.port), 10);
    // this is dumb, but the mqtt client doesn't have a connect method. you need to iterate the
    // connection for it to do things. without this, nothing will happen
    thread::spawn(move || {
        for notification in connection.iter() {
            trace!("got mqtt notification: {:?}", notification);
        }
    });

    info!("successfully connected to mqtt broker");
    if let Err(e) = client.subscribe(&args.topic, QoS::AtLeastOnce) {
        bail!("unable to subscribe to '{}'. {:?}", &args.topic, e);
    }

    let (sink_chan, rx) = mpsc::channel();
    thread::spawn(move || mqtt_publisher(args.topic.clone(), client, rx));

    // read data from the port
    let mut buf: Vec<u8> = vec![0; 1024];
    let poll_duration = Duration::from_millis(1000);
    let mut listener = Listener { sink_chan };
    let mut parser = vedirect::Parser::new(&mut listener);

    info!("starting vedirect reader");
    loop {
        thread::sleep(poll_duration);

        let r = port.read(buf.as_mut_slice());
        if r.is_err() {
            error!("unable to read from port. {:?}", r.err().unwrap());
            continue;
        }

        let r = r.unwrap();
        trace!("read {} bytes from port", &r);
        parser.feed(&buf[..r])?;
    }
}
