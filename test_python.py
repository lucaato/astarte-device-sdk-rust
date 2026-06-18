from builddir.AstarteDeviceLib import DeviceConfig

async def send_data_loop(device):
    i = 0
    while True:
        i += 1

        # await device.send_data("org.astarte-platform.rust.examples.individual-datastream.DeviceDatastream", "/endpoint1", i)

        await asyncio.sleep(20.0)

async def receive_data_loop(device):
    while True:
        print("receiving data")

        event = await device.receive_data()

        print("received event of type", event.interface_name, event.path, event.get_type())

async def main(device):
    asyncio.create_task(receive_data_loop(device))

    await send_data_loop(device)

if __name__ == "__main__":
    config = DeviceConfig()
    config.device_id = "DayugqhpTPi2RgkELFPj9Q"
    config.cred_secr = "hV96foZQApU+J086iHN1F/Q/siVvBD1znIQW7UrOosU="
    config.realm = "test"
    config.pairing_url = "http://api.astarte.localhost/pairing"
    config.interfaces_dir="../../examples/individual_datastream/interfaces"

    device.start()

    asyncio.run(main(device))

    print("stopping device...", flush=True)
    device.stop()
