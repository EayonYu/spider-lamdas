from enum import Enum, unique


@unique
class EventType(Enum):
    DeviceAdded = 'DeviceAdded'
    DeviceDeleted = 'DeviceDeleted'
    DeviceInfoChanged = 'DeviceInfoChanged'
