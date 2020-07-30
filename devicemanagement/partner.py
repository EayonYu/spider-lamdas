from enum import Enum, unique


@unique
class PartnerType(Enum):
    ChinaIoT = 'china_iot'
    OverseaIoT = 'oversea_iot'
