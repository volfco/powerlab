use clap::Parser;
use log::{debug, error, info, trace};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_postgres::NoTls;

type DbChannelType = (String, serde_json::Value);

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// MQTT broker address
    #[clap(short, long)]
    mqtt_broker: String,

    /// MQTT Broker port
    #[clap(short, long, default_value = "1883")]
    mqtt_port: u16,

    /// MQTT Broker ID
    #[clap(short, long, default_value = "mqtt_consumer")]
    mqtt_id: String,

    /// MQTT Broker Keep Alive
    #[clap(short, long, default_value = "60")]
    mqtt_keep_alive: u16,

    /// Postgresql connection string
    #[clap(
        short,
        long,
        default_value = "postgresql://postgres:postgres@localhost:5432/postgres"
    )]
    db: String,

    /// Topics to subscribe to. Each argument is a comma separated map of topic to table.
    /// Example: iot-devices/solar,solar-power will consume the topic iot-devices/solar and write
    /// all messages to the table solar-power.
    #[clap(short, long)]
    topics: Vec<String>,
}

fn parse_topics(topics: Vec<String>) -> Vec<(String, String)> {
    topics
        .iter()
        .map(|topic| {
            let mut split = topic.split(',');
            (
                split.next().unwrap().to_string(), // topic
                split.next().unwrap().to_string(), // table
            )
        })
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    // connect to postgres
    let (db_client, _) = tokio_postgres::connect(&args.db, NoTls).await?;
    info!("connected to postgres");

    // connect to the mqtt broker
    let mut mqttoptions = MqttOptions::new(args.mqtt_id, args.mqtt_broker, args.mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(args.mqtt_keep_alive.into()));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    info!("connected to mqtt broker");

    // Start database writer routine
    let (db_tx, mut db_rx): (Sender<DbChannelType>, Receiver<DbChannelType>) = channel(32);

    tokio::spawn(async move {
        while let Some((topic, payload)) = db_rx.recv().await {
            // unwrap payload and get the keys, values and convert to an insert statement
            let payload = payload.as_object().unwrap();
            let keys = payload
                .keys()
                .map(|k| k.to_string())
                .collect::<Vec<String>>();
            let values = payload
                .values()
                .map(|v| v.to_string())
                .collect::<Vec<String>>();

            let table = topic.replace('/', "_");

            let mut var_string = String::new();
            for i in 1..keys.len() {
                var_string.push_str(&format!("${i}, "));
            }

            let stmt_sql = format!(
                "INSERT INTO {} (timestamp, {}) VALUES (NOW(), {})",
                &table,
                &keys.join(", "),
                &var_string
            );

            debug!("Executing Prepared Statement SQL: {}", stmt_sql);

            let stmt = db_client.prepare(&stmt_sql).await.unwrap();

            db_client.execute_raw(&stmt, values).await.unwrap();
        }
    });

    // parse the topics into a vector of tuples that we can use
    let topics = parse_topics(args.topics);
    info!("parsed topics: {:?}", topics);

    // subscribe to all the given topics
    for (topic, _) in topics.iter() {
        debug!("Subscribing to topic: {}", topic);
        client.subscribe(topic, QoS::AtLeastOnce).await?;
    }

    // now, wait for mqtt messages to be recieved and when they are- push them to the database writer channel
    loop {
        if let Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) =
            eventloop.poll().await
        {
            trace!("got publish notification: {:?}", &publish);
            let payload = String::from_utf8(publish.payload.to_vec()).unwrap();
            let payload: serde_json::Value = match serde_json::from_str(&payload) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error parsing packet as json: {}", e);
                    continue;
                }
            };

            db_tx.send((publish.topic, payload)).await.unwrap();
        }
    }
}
