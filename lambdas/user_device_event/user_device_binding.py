# coding:utf-8
"""
description:
"""
import datetime

from sqlalchemy import and_
from layer.model import PartnerUserModel
from layer.model import PartnerDeviceModel

from layer.model import GaiaUserDeviceBindings
from layer.model import PartnerUserDeviceBindings


def user_device_binding_data(data: list, session):
    # 先把list的 userId 和deviceId 提取到两个list中
    # 再把两个list 查bingings
    # 得到本地数据库中的bindings，再比较传过来的增删改

    # 先根据两个list 查询可能用到的用户和设备 提取两个gaia的用户(包含三个字段)和设备（包含三个字段）list
    # 改的过程中对gaia bindings 做增删改
    #     [{'deviceId': '2030838', 'userId': '2030837', 'userRole': '1', 'partnerId': 'china_iot', 'messageType': 'UserDeviceBinding.Event'}]
    #     分组

    try:
        target_partner_user_list = []
        target_partner_binding_compound_key_dict = {}
        group_by_user = {}
        partner_id = data[0].get("partnerId")

        for target_binding in data:
            target_partner_user_list.append(target_binding["userId"])
            key = str(target_binding["userId"]) + ":" + str(target_binding["deviceId"])
            value = target_binding
            target_partner_binding_compound_key_dict.update({
                key: value
                })
        for target_partner_user_id in target_partner_user_list:
            each_target_user_device_map_list = []
            for target_binding in data:
                if target_binding["userId"] == target_partner_user_id:
                    each_target_user_device_map_list.append(
                            target_binding["deviceId"])
            group_by_user.update({
                target_partner_user_id: each_target_user_device_map_list
                })
        # 得到分组json obj dict {'2030837': ['2030838']}
        print(group_by_user)

        for target_partner_user_id in target_partner_user_list:
            target_partner_device_id_list = group_by_user.get(target_partner_user_id)

            current_local_bindings = session.query(PartnerUserDeviceBindings).filter(
                    and_(
                            PartnerUserDeviceBindings.partner_user_id == target_partner_user_id,
                            PartnerUserDeviceBindings.partner_id == partner_id)).all()
            # 循环设备
            for target_partner_device_id in target_partner_device_id_list:
                key = str(target_partner_user_id) + ":" + str(target_partner_device_id)
                find = False
                for current_local_binding in current_local_bindings:
                    if current_local_binding.partner_device_id == target_partner_device_id:
                        find = True
                        # 修改 （不论是恢复还是修改都要修改绑定的额外信息）
                        current_local_binding.updated_at = True
                        current_local_binding.updated_re = datetime.datetime.utcnow()
                        current_local_binding.user_role = target_partner_binding_compound_key_dict.get(key).get(
                            "user_role",
                            None)
                        current_local_binding.extra = target_partner_binding_compound_key_dict.get(key).get("extra",
                                                                                                            None)

                        if current_local_binding.deleted_at:
                            # 恢复（新增）
                            current_local_binding.deleted_at = None
                        else:
                            # 修改
                            pass
                        break

                if not find:
                    # 新增
                    partner_binding = PartnerUserDeviceBindings()
                    partner_binding.update_required = True
                    partner_binding.created_at = datetime.datetime.utcnow()
                    partner_binding.partner_id = partner_id
                    partner_binding.partner_device_id = target_partner_device_id
                    partner_binding.partner_user_id = target_partner_user_id
                    partner_binding.extra = target_partner_binding_compound_key_dict.get(key).get("extra", None)
                    partner_binding.user_role = target_partner_binding_compound_key_dict.get(key).get("user_role",
                                                                                                      None)
                    session.add(partner_binding)

            for current_local_binding in current_local_bindings:
                find = False
                for target_partner_device_id in target_partner_device_id_list:
                    if current_local_binding.partner_device_id == target_partner_device_id:
                        find = True
                        break
                if not find:
                    # 删除
                    current_local_binding.deleted_at = datetime.datetime.utcnow()
            session.flush()

            # 开始同步gaia bindings

            # 查partner用户对应的平台用户
            platform_user = session.query(PartnerUserModel).filter(
                    and_(
                            PartnerUserModel.deleted_at.is_(None),
                            PartnerUserModel.partner_user_id == target_partner_user_id,
                            PartnerUserModel.partner_id == partner_id,
                            )
                    ).first()
            if not platform_user:
                # 没有该用户 不需要再进行了
                break

            platform_user_id = platform_user.platform_user_id
            # 查本地partner 用户已经绑定的设备列表 此处必须得返回结果（因为前面已经添加过了），否则就是传过来的空列表
            current_local_binding_list = session.query(PartnerUserDeviceBindings).filter(
                    and_(
                            PartnerUserDeviceBindings.deleted_at.is_(None),
                            PartnerUserDeviceBindings.partner_user_id == target_partner_user_id,
                            PartnerUserDeviceBindings.partner_id == partner_id,
                            )
                    ).all()
            # 查询全部的 partnerDeviceIds 返回值中包含（partner_device_id,partner_id,platform_device_id）
            partner_device_list: list = session.query(PartnerDeviceModel).filter(
                    and_(
                            PartnerDeviceModel.partner_device_id.in_(target_partner_device_id_list),
                            PartnerDeviceModel.partner_id == partner_id
                            )
                    ).all()

            # 组装target_bindings
            target_binding_dict_list = []

            for partner_device in partner_device_list:
                # partner_device 中包含 platform_device_id
                for current_local_binding in current_local_binding_list:
                    # 把 platform_device_id 赋值给 current_local_binding （current_local_binding_list 和 用户此次绑定的 user_device 数量是相等的 ）
                    binding_dict = current_local_binding.dict()
                    if current_local_binding.partner_id == partner_device.partner_id and \
                            current_local_binding.partner_device_id == partner_device.partner_device_id:
                        #             把platform_device_id 封装给current_local_binding
                        binding_dict.update({
                            "platform_device_id": partner_device.platform_device_id
                            })
                        target_binding_dict_list.append(binding_dict)

            # for current_local_binding in current_local_binding_list:
            #     binding_dict = current_local_binding.dict()
            #     for partner_device in partner_device_list:
            #         if current_local_binding.partner_id == partner_device.partner_id and \
            #                 current_local_binding.partner_device_id == partner_device.partner_device_id:
            #             binding_dict.update({
            #                 "platform_device_id": partner_device.platform_device_id
            #                 })
            #             target_binding_dict_list.append(binding_dict)
            #             把platform_device_id 封装给current_local_binding

            # 查改用户现有gaia bindings
            current_local_gaia_bindings = session.query(GaiaUserDeviceBindings).filter(
                    and_(
                            GaiaUserDeviceBindings.platform_user_id == platform_user_id
                            )
                    ).all()
            for target_binding_dict in target_binding_dict_list:
                find = False
                for current_local_gaia_binding in current_local_gaia_bindings:
                    if target_binding_dict["platform_device_id"] == current_local_gaia_binding.platform_device_id:
                        find = True
                        current_local_gaia_binding.user_role = target_binding_dict["user_role"]
                        current_local_gaia_binding.extra = target_binding_dict["extra"]
                        current_local_gaia_binding.updated_at = datetime.datetime.utcnow()
                        if current_local_gaia_binding.deleted_at:
                            # 恢复（新增）
                            current_local_gaia_binding.deleted_at = None
                        else:
                            # 修改 把修改提取到前面去了
                            pass

                        #  修改update_required
                        current_local_partner_binding = session.query(PartnerUserDeviceBindings).filter(
                                and_(
                                        PartnerUserDeviceBindings.deleted_at.is_(None),
                                        PartnerUserDeviceBindings.partner_user_id == target_partner_user_id,
                                        PartnerUserDeviceBindings.partner_device_id == target_binding_dict[
                                            "partner_device_id"],
                                        )
                                ).first()
                        current_local_partner_binding.update_required = False
                        current_local_partner_binding.gaia_user_device_unique_id = current_local_gaia_binding.id
                        break
                if not find:
                    # 新增
                    new_binding = GaiaUserDeviceBindings()
                    new_binding.platform_user_id = platform_user_id,
                    new_binding.platform_device_id = target_binding_dict["platform_device_id"],
                    new_binding.user_role = target_binding_dict["user_role"],
                    new_binding.extra = target_binding_dict["extra"],
                    new_binding.created_at = datetime.datetime.utcnow(),
                    session.add(new_binding)
                    session.flush()

                    #  修改update_required
                    current_local_partner_binding = session.query(PartnerUserDeviceBindings).filter(
                            and_(
                                    PartnerUserDeviceBindings.deleted_at.is_(None),
                                    PartnerUserDeviceBindings.partner_user_id == target_partner_user_id,
                                    PartnerUserDeviceBindings.partner_device_id == target_binding_dict[
                                        "partner_device_id"],

                                    )
                            ).first()
                    current_local_partner_binding.update_required = False
                    current_local_partner_binding.gaia_user_device_unique_id = new_binding.id

            for current_local_gaia_binding in current_local_gaia_bindings:
                find = False
                for target_binding_dict in target_binding_dict_list:
                    if target_binding_dict["platform_device_id"] == current_local_gaia_binding.platform_device_id:
                        find = True
                        break
                if not find:
                    # 删除
                    current_local_gaia_binding.deleted_at = datetime.datetime.utcnow()
    except Exception as e:
        print(e)
        pass

    session.commit()
