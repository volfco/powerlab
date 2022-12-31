mod schema_guesser;

use clap::Parser;
use log::{debug, error, info, trace, warn};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
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

    /// Attempt to guess the schema of the incoming data for topics in which the associated table
    /// does not exist.
    ///
    /// If the topic `foo` is provided, but the `foo` table does not exist, the schema will be guessed
    /// based off the first message this consumer receives.
    ///
    /// If the first message from the `foo` topic is:
    ///     {"bar": 1, "baz": 2}
    /// The resulting table will look like:
    ///    CREATE TABLE foo (timestamp DATETIME, bar int, baz int);
    ///
    /// If the topic `foo` is provided, and the `foo` table exists, the schema will not be guessed.
    #[clap(long)]
    db_guess_schema: bool,

    /// If guessing the schema, convert the generated table to a Timeseries table.
    ///
    /// Options are:
    ///   - hypertable: Create a hypertable (TimeseriesDB)
    ///
    /// If not provided, no conversion will be performed and a standard table will be used.
    #[clap(long)]
    db_guess_schema_timeseries: Option<String>,

    /// Topics to subscribe to. Each argument is a comma separated map of topic to table.
    /// Example: iot-devices/solar,solar-power will consume the topic iot-devices/solar and write
    /// all messages to the table solar-power.
    #[clap(short, long)]
    topics: Vec<String>,
}

fn parse_topics(topics: Vec<String>) -> BTreeMap<String, String> {
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

/// Check if the table
async fn check_topic_table(pool: Pool<PostgresConnectionManager<NoTls>>, table: &String) -> bool {
    let conn = pool.get().await.unwrap();
    let select = conn
        .prepare(
            "SELECT EXISTS (
    SELECT FROM 
        pg_tables
    WHERE 
        schemaname = 'public' AND 
        tablename  = '$1'
    );",
        )
        .await
        .unwrap();
    let row = conn.query_one(&select, &[&table]).await.unwrap();
    row.get::<usize, bool>(0)
}

async fn message_handler(
    pool: Pool<PostgresConnectionManager<NoTls>>,
    table: &String,
    payload: serde_json::Value,
) {
    let conn = pool.get().await.unwrap();
    trace!("Writing '{payload:?} to table '{table}");

    let payload = payload.as_object().unwrap();
    let keys = payload
        .keys()
        .map(|k| k.to_string())
        .collect::<Vec<String>>();
    let values = payload
        .values()
        .map(|v| v.to_string())
        .collect::<Vec<String>>();

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

    let stmt = conn.prepare(&stmt_sql).await.unwrap();
    conn.execute_raw(&stmt, values).await.unwrap();
}

/// Consume messages from the MQTT broker channel and write them to the database.
async fn message_consumer(
    pool: Pool<PostgresConnectionManager<NoTls>>,
    mut rx: Receiver<DbChannelType>,
    topic_map: BTreeMap<String, String>,
    guess_schema: bool,
) {
    let mut seen_topics = vec![];
    while let Some((topic, payload)) = rx.recv().await {
        let target_table = topic_map.get(&topic).unwrap();
        // check if we've already seen a message for this topic. if not, we should check if the
        // table exists.
        if !seen_topics.contains(&topic) {
            warn!("Topic {topic} has not been seen before. Checking if table exists.");
            if check_topic_table(pool.clone(), target_table).await {
                info!("Table for topic {topic} exists. Continuing.");
                seen_topics.push(topic.clone());
            } else {
                if !guess_schema {
                    warn!("Table for topic {topic} does not exist. Guessing schema is disabled. Dropping message.");
                    continue;
                }
                warn!("Table for topic {topic} does not exist. Attempting to guess schema.");
                let schema = schema_guesser::generate_schema_sql(target_table, &payload);
                let conn = pool.get().await.unwrap();
                if let Err(e) = conn.execute(&schema, &[]).await {
                    error!("Failed to create table for topic {}: {}", &topic, e);
                    continue;
                }
                seen_topics.push(topic.clone());
                info!("Table for topic {topic} created. Continuing.");
            }
        }

        message_handler(pool.clone(), target_table, payload).await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    // connect to postgres
    let manager = PostgresConnectionManager::new(
        tokio_postgres::config::Config::from_str(args.db.as_str())?,
        NoTls,
    );
    let pool = Pool::builder().build(manager).await?;

    // connect to the mqtt broker
    let mut mqttoptions = MqttOptions::new(args.mqtt_id, args.mqtt_broker, args.mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(args.mqtt_keep_alive.into()));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    info!("connected to mqtt broker");

    // subscribe to all the given topics
    let topic_map = parse_topics(args.topics);
    for (topic, _) in topic_map.iter() {
        client.subscribe(topic, QoS::AtLeastOnce).await?;
        debug!("subscribed to topic {topic}");
    }

    // Start database writer routine
    let (db_tx, db_rx): (Sender<DbChannelType>, Receiver<DbChannelType>) = channel(32);
    let consumer_pool = pool.clone();
    tokio::spawn(async move {
        message_consumer(consumer_pool, db_rx, topic_map, args.db_guess_schema).await
    });

    // now, wait for mqtt messages to be received and when they are- push them to the database
    // writer channel
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
