from layer import Layer, Env

from devicemanagement.device_cosume import consume, _sync_partner_device_add, _sync_gaia_device_add


def test01():
    data = [{'deviceId': '2030837',
             'reachability': {'deviceId': '2030837', 'value': 'online', 'updated_at': 1593672252675},
             'deviceInfo': {'deviceId': '2030837', 'nickName': '空调333', 'tslId': 'tsl-id-01-temp02',
                            'deviceType': 'DEVICE-AC', 'manufacturer': 'TCL', 'model': 'AC',
                            'mac': '38:76:CA:44:74:27',
                            'serialNo': '7635798hiohy986289369',
                            'firmwareVersions': {'wifiModule': '123', 'mcu': '2331'},
                            'tenantId': 'TCL-2C', 'protocol': 'WiFi',
                            'geolocation': {'longitude': 123.0, 'latitude': 43.94},
                            'location': {'room': 'parent-bedroom', 'floor': '1'},
                            'deviceIcons': {'32dp': '233232.png', '64dp': 'wewe.png'},
                            'extra': {'SSID': 'aaaaaa', 'BT-name': 'bbb'}},
             'properties': {'deviceId': '2030837', 'capabilities': {}}, 'partnerId': 'china_iot',
             'messageType': 'DeviceAdded.Event'}]
    delete_message = {
        'deviceId': '2030836',
        'messageType': 'DeviceDeleted.Event',
        'partnerId': 'china_iot',
    }
    data1 = [delete_message]
    print(type(data))
    layer = Layer(Env.DEV)
    consume(data1, layer)


if __name__ == '__main__':
    # layer = Layer(Env.DEV)
    # _sync_gaia_device_add(layer)
    test01()
