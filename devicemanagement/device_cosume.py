import datetime
import json
import uuid

from layer import Layer
from layer.model import GaiaDeviceModel, PartnerDeviceModel

from sqlalchemy import and_

from .event_type import EventType
from .partner import PartnerType
from .utils import generate_immutable_identity, ImmutableIdentityElement


def consume(data, layer: Layer):
    for message in data:
        try:
            event_type = message['messageType'].split('.')[0]
            _handler(message, event_type, layer)
        except KeyError:
            # TODO
            pass


def _handler(data, event_type: EventType, layer: Layer):
    if event_type == EventType.DeviceAdded.value:
        _device_add_handler(data, layer)
    elif event_type == EventType.DeviceDeleted.value:
        _device_delete_handler(data, layer)
    elif event_type == EventType.DeviceInfoChanged.value:
        _device_info_changed_handler(data, layer)
    else:
        return {

        }


def _device_add_handler(data, layer):
    immutable_identity = _get_immutable_identity(data)
    partner_id = data['partnerId']
    partner_device_id = data['deviceId']
    if immutable_identity:
        try:
            session = layer.session()
            # 根据immutable_identity查询是否在这个partner中已经存在这个设备，如果存在，说明这个设备可能是恢复了出厂设置
            partner_device = session.query(PartnerDeviceModel).filter(
                and_(
                    PartnerDeviceModel.partner_id == partner_id,
                    PartnerDeviceModel.immutable_identity == immutable_identity
                )
            ).one_or_none()
            if partner_device and not partner_device.deleted_at:
                partner_device.deleted_at = datetime.datetime.utcnow()
                # 判断这个设备是否是primary设备，如果是的话，需要把gaia_device中保存的设备设置为删除
                if partner_device.primary_device:
                    gaia_device = session.query(GaiaDeviceModel).filter(
                        and_(
                            GaiaDeviceModel.immutable_identity == immutable_identity
                        )
                    ).one_or_none()
                    if gaia_device and not gaia_device.delete_at:
                        gaia_device.delete_at = datetime.datetime.utcnow()

            # 在partner_device增加该设备
            partner_device = PartnerDeviceModel()
            partner_device.immutable_identity = immutable_identity
            partner_device.partner_id = partner_id
            partner_device.partner_device_id = partner_device_id
            partner_device.device_info = json.dumps(data['deviceInfo'])
            partner_device.update_required = True

            session.add(partner_device)
            session.commit()
            # sync partner_device
            _sync_partner_device_add(layer)
        except Exception as e:
            session.rollback()
            raise e

    else:
        pass


def _sync_partner_device_add(layer: Layer):
    session = layer.session()
    try:
        partner_devices = session.query(PartnerDeviceModel).filter(
            and_(
                PartnerDeviceModel.update_required == True
            )
        ).all()
        for partner_device in partner_devices:
            if not partner_device.deleted_at:
                immutable_identity = partner_device.immutable_identity
                gaia_device = session.query(GaiaDeviceModel).filter(
                    and_(GaiaDeviceModel.immutable_identity == immutable_identity)
                ).one_or_none()
                # gaia_device存在且没有删除，直接建立mapping关系
                if gaia_device and not gaia_device.delete_at:
                    gaia_device.update_required = True
                else:
                    # gaia_device不存在，新增一个设备
                    gaia_device = GaiaDeviceModel()
                    gaia_device.platform_device_id = str(uuid.uuid1())
                    gaia_device.update_required = True
                    gaia_device.immutable_identity = immutable_identity
                    gaia_device.device_info = partner_device.device_info
                    session.add(gaia_device)
                partner_device.update_required = False
                platform_device_id = gaia_device.platform_device_id
                partner_device.platform_device_id = platform_device_id
        session.commit()

        _sync_gaia_device_add(layer)
    except Exception as e:
        session.rollback()
        raise e


def _sync_gaia_device_add(layer: Layer):
    try:
        session = layer.session()
        gaia_devices = session.query(GaiaDeviceModel).filter(
            and_(
                GaiaDeviceModel.update_required == True
            )
        ).all()
        for gaia_device in gaia_devices:
            if not gaia_device.deleted_at:
                # decide and mark new PRIMARY device for this platform_device
                partner_devices = session.query(PartnerDeviceModel).filter(
                    and_(
                        PartnerDeviceModel.immutable_identity == gaia_device.immutable_identity
                    )
                ).all()

                def is_close_partner(partner_device):
                    partner_id = partner_device.partner_id
                    return partner_id == PartnerType.ChinaIoT.value or partner_id == PartnerType.OverseaIoT.value

                close_partner_devices = list(filter(is_close_partner, partner_devices))
                if len(close_partner_devices) > 0:
                    sorted(close_partner_devices, key=lambda d: d.updated_at, reverse=True)[0].primary_device = True
                else:
                    sorted(partner_devices, key=lambda d: d.updated_at, reverse=True)[0].primary_device = True
                gaia_device.update_required = False
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def _device_delete_handler(data, layer: Layer):
    try:
        partner_id = data['partnerId']
        partner_device_id = data['deviceId']
        session = layer.session()
        partner_device = session.query(PartnerDeviceModel).filter(
            and_(
                PartnerDeviceModel.partner_id == partner_id,
                PartnerDeviceModel.partner_device_id == partner_device_id
            )
        ).one_or_none()
        if partner_device and not partner_device.deleted_at:
            partner_device.deleted_at = datetime.datetime.utcnow()
            gaia_device = session.query(GaiaDeviceModel).filter(
                and_(
                    GaiaDeviceModel.immutable_identity == partner_device.immutable_identity
                )
            ).one_or_none()
            if partner_device.primary_device:
                partner_device.primary_device = False
                gaia_device.deleted_at = datetime.datetime.utcnow()

            # unmap
            partner_device.platform_device_id = None
            gaia_device.update_required = True

            session.commit()

            _sync_gaia_device_deleted(layer)
    except Exception as e:
        session.rollback()
        raise e


def _sync_gaia_device_deleted(layer: Layer):
    _sync_gaia_device_add(layer)


def _device_info_changed_handler(data, layer: Layer):
    try:
        partner_id = data['partnerId']
        partner_device_id = data['deviceId']
        session = layer.session()
        partner_device = session.query(PartnerDeviceModel).filter(
            and_(
                PartnerDeviceModel.partner_id == partner_id,
                PartnerDeviceModel.partner_device_id == partner_device_id
            )
        ).one_or_none()
        if partner_device and not partner_device.deleted_at:
            device_info_update = data['deviceInfo']
            device_info = json.loads(partner_device.device_info)
            for key in device_info_update:
                device_info[key] = device_info_update[key]
            partner_device.device_info = json.dumps(device_info)
            if partner_device.primary_device:
                gaia_device = session.query(GaiaDeviceModel).filter(
                    and_(
                        GaiaDeviceModel.immutable_identity == partner_device.immutable_identity
                    )
                )
                gaia_device.device_info = partner_device.device_info

        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def _get_immutable_identity(data) -> str:
    device_info = data['deviceInfo']
    partner_id = data['partnerId']
    partner_device_id = data['deviceId']

    immutable_identity_element = ImmutableIdentityElement()
    immutable_identity_element.manufacturer = device_info['manufacturer']
    immutable_identity_element.product_type = device_info['deviceType']
    immutable_identity_element.sn = device_info['serialNo']
    immutable_identity_element.mac = device_info['mac']
    immutable_identity_element.partner_id = partner_id
    immutable_identity_element.partner_device_id = partner_device_id
    immutable_identity = generate_immutable_identity(immutable_identity_element)

    return immutable_identity
