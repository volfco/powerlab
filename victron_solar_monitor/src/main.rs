use anyhow::{bail, Context};
use clap::Parser;
use log::{debug, error, info, trace};
use rumqttc::{Client, MqttOptions, QoS};
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::thread;
use std::time::Duration;
use vedirect::{Events, VEError, MPPT};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[clap(short, long)]
    device: String,

    /// MQTT broker address
    #[clap(short, long)]
    mqtt_broker: String,

    /// MQTT Broker port
    #[clap(short, long, default_value = "1883")]
    mqtt_port: u16,

    /// MQTT Broker ID
    #[clap(short, long, default_value = "victron_solar_monitor")]
    mqtt_id: String,

    /// MQTT Broker topic
    #[clap(short, long, default_value = "victron_solar_monitor")]
    mqtt_topic: String,

    /// MQTT Broker Keep Alive
    #[clap(short, long, default_value = "60")]
    mqtt_keep_alive: u16,
}

struct Listener {
    sink_chan: mpsc::Sender<MPPT>,
}

impl Events<MPPT> for Listener {
    fn on_complete_block(&mut self, block: MPPT) {
        trace!("read data {:#?}", &block);
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
    loop {
        let block = rx.recv().unwrap();
        let payload = serde_json::to_string(&block).unwrap();
        trace!("published to topic '{}'. payload : {:?}", &topic, &payload);
        if let Err(e) = client.publish(&topic, QoS::AtLeastOnce, false, payload) {
            error!("unable to publish to topic '{}'. {:?}", &topic, e);
        }
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    debug!("parsed args: {:#?}", &args);

    let mut port = serialport::new(args.device.clone(), 19_200)
        .data_bits(serialport::DataBits::Eight)
        .timeout(core::time::Duration::from_secs(2))
        .open()
        .context("Failed to open vedirect serial port")?;
    info!("successfully opened serial port: {}", args.device);

    // connect to mqtt broker
    let mut mqttoptions = MqttOptions::new(args.mqtt_id, args.mqtt_broker, args.mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(args.mqtt_keep_alive.into()));
    mqttoptions.set_transport(rumqttc::Transport::Tcp);

    let (mut client, mut connection) = Client::new(mqttoptions, 10);
    // this is dumb, but the mqtt client doesn't have a connect method. you need to iterate the
    // connection for it to do things. without this, nothing will happen
    thread::spawn(move || {
        for notification in connection.iter() {
            trace!("got mqtt notification: {:?}", notification);
        }
    });
    info!("successfully connected to mqtt broker");

    if let Err(e) = client.subscribe(&args.mqtt_topic, QoS::AtLeastOnce) {
        bail!("unable to subscribe to topic: {:?}", e);
    }

    let (sink_chan, rx) = mpsc::channel();
    thread::spawn(move || mqtt_publisher(args.mqtt_topic.clone(), client, rx));

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
        debug!("read {} bytes from port", &r);
        parser.feed(&buf[..r])?;
    }
}
