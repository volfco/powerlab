use btleplug::api::{
    bleuuid::uuid_from_u16, Central, Manager as _, Peripheral as _, ScanFilter, WriteType,
};
use btleplug::platform::{Adapter, Manager, Peripheral};
use log::{debug, info, trace};
use rand::{thread_rng, Rng};
use std::error::Error;
use std::time::Duration;
use uuid::Uuid;

const LIGHT_CHARACTERISTIC_UUID: Uuid = uuid_from_u16(0xFFE9);
use tokio::time;

async fn find_light(central: &Adapter) -> Option<Peripheral> {
    for p in central.peripherals().await.unwrap() {
        let name = p.properties().await.unwrap().unwrap().local_name;

        if p.properties()
            .await
            .unwrap()
            .unwrap()
            .local_name
            .iter()
            .any(|name| name.contains("LEDBlue"))
        {
            return Some(p);
        }
    }
    None
}

async fn discover_devices(
    central: &Adapter,
    device_names: &Vec<String>,
) -> Vec<(String, Peripheral)> {
    let mut devices = Vec::new();
    for p in central.peripherals().await.unwrap() {
        let device = p.properties().await.unwrap().unwrap();
        if device.local_name.is_none() {
            trace!("device {} has no name, ignoring", device.address);
            continue;
        }

        let name = device.local_name.unwrap();
        if device_names.contains(&name) {
            debug!("found device {} with name {}", device.address, name);
            devices.push((name, p));
        }
    }

    devices
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    let manager = Manager::new().await.unwrap();

    // get the first bluetooth adapter
    let central = manager
        .adapters()
        .await
        .expect("Unable to fetch adapter list.")
        .into_iter()
        .nth(0)
        .expect("Unable to find adapters.");

    // start scanning for devices
    central.start_scan(ScanFilter::default()).await?;
    // instead of waiting, you can use central.events() to get a stream which will
    // notify you of new devices, for an example of that see examples/event_driven_discovery.rs
    time::sleep(Duration::from_secs(5)).await;

    let devices = discover_devices(
        &central,
        &vec![
            "DL-401607013017".to_string(),
            "DL-401607013083".to_string(),
            "DL-401607012915".to_string(),
        ],
    )
    .await;

    // connect to the first one for now
    let (name, device) = devices.get(0).unwrap();
    info!("connecting to device {}", name);
    device.connect().await?;

    // get the service
    device.discover_services().await?;

    let chars = device.characteristics();
    let write_char = chars
        .iter()
        .find(|c| c.uuid == uuid_from_u16(15))
        .expect("Unable to find characteristics");

    let read_char = chars
        .iter()
        .find(|c| c.uuid == uuid_from_u16(17))
        .expect("Unable to find characteristics");

    let message = format!("a5{}0{}08{}", 8, 94, "");
    let message = format!("{:0<24}", message);
    let bytes = hex::decode(message).unwrap();

    let crc: u8 = bytes.iter().sum::<u8>() & 0xFF;

    device
        .write(write_char, &bytes, WriteType::WithResponse)
        .await?;

    let resp = device.read(read_char).await?;
    println!("resp: {resp:?}");

    // send command 94 to read status

    //
    // // find the device we're interested in
    // let light = find_light(&central).await.expect("No lights found");
    //
    // // connect to the device
    // light.connect().await?;
    //
    // // discover services and characteristics
    // light.discover_services().await?;
    //
    // // find the characteristic we want
    // let chars = light.characteristics();
    // let cmd_char = chars
    //     .iter()
    //     .find(|c| c.uuid == LIGHT_CHARACTERISTIC_UUID)
    //     .expect("Unable to find characterics");
    //
    // // dance party
    // let mut rng = thread_rng();
    // for _ in 0..20 {
    //     let color_cmd = vec![0x56, rng.gen(), rng.gen(), rng.gen(), 0x00, 0xF0, 0xAA];
    //     light
    //         .write(&cmd_char, &color_cmd, WriteType::WithoutResponse)
    //         .await?;
    //     time::sleep(Duration::from_millis(200)).await;
    // }
    Ok(())
}
