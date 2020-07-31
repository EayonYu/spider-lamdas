from layer import Layer, Env
import unittest
from devicemanagement.device_cosume import consume_device_event, _sync_partner_device_add, _sync_gaia_device_add, _sync_gaia_device_deleted


def test01():
    delete_message = {
        'deviceId': '2030836',
        'messageType': 'DeviceDeleted.Event',
        'partnerId': 'china_iot',
    }
    data1 = [delete_message]
    layer = Layer(Env.DEV)
    consume_device_event(data1, layer)


def test_add_partner_device():
    layer = Layer(Env.DEV)
    device_add_message = {'deviceId': '2030839',
                          'reachability': {'deviceId': '2030839', 'value': 'online', 'updated_at': 1593672252675},
                          'deviceInfo': {'deviceId': '2030839', 'nickName': '空调333', 'tslId': 'tsl-id-01-temp02',
                                         'deviceType': 'DEVICE-AC', 'manufacturer': 'TCL', 'model': 'AC',
                                         'mac': '38:76:CA:44:74:27',
                                         'serialNo': '90877986532986289369',
                                         'firmwareVersions': {'wifiModule': '123', 'mcu': '2331'},
                                         'tenantId': 'TCL-2C', 'protocol': 'WiFi',
                                         'geolocation': {'longitude': 123.0, 'latitude': 43.94},
                                         'location': {'room': 'parent-bedroom', 'floor': '1'},
                                         'deviceIcons': {'32dp': '233232.png', '64dp': 'wewe.png'},
                                         'extra': {'SSID': 'aaaaaa', 'BT-name': 'bbb'}},
                          'properties': {'deviceId': '2030839', 'capabilities': {}}, 'partnerId': 'china_iot',
                          'messageType': 'DeviceAdded.Event'}
    device_delete_message = {
        'deviceId': '2030836',
        'messageType': 'DeviceDeleted.Event',
        'partnerId': 'china_iot',
    }
    data = [device_add_message]
    consume(data, layer)


if __name__ == '__main__':
    layer = Layer(Env.DEV)
    # test_add_partner_device()
    # _sync_gaia_device_add(layer)
    _sync_partner_device_add(layer)