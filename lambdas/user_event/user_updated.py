'''
如果没有 immutable_identity：
    组装 immutable_identity
根据immutable_identity & deleted is null 查
    如果 查到了 看partnerId 和partnerUserId 是和传过来的否一致，
        如果 不一致要修改partnerUserId,和基础信息

    如果不存在，什么都不做，
'''
import json

from layer.model import GaiaUserModel
from layer.model import PartnerUserModel
from sqlalchemy import and_

from lambdas.user_event.user_deleted1 import process_login_details


def user_update_data(data: list, session):
    """
    删除partner user
    :param data:
    :type data:
    :param session:
    :type session:
    :return:
    :rtype:
    """
    # UserAdded.event
    # ''' [{'userId': '2030836', 'immutableIdentity': 'tcl-sso:', 'userName': 'Ethan', 'mobile': '13120575591',
    #    'email': 'yuyoung@613.com', 'login_details': [{'accountSystemId': 'tcl-sso', 'loginAccountId': '23222233'}],
    #    'tenantId': '', 'idType': 'passport', 'idNo': '2121212111', 'partnerId': 'china_iot',
    #    'messageType': 'UserAdded.command'}]'''
    #     删除
    immutable_identity_list = []
    target_partner_user_list = []
    for partner_user in data:

        if not partner_user.get("immutableIdentity"):
            immutable_identity = str(partner_user.get("partnerId") + ":" + str(partner_user.get("userId")))
            partner_user.update({"immutableIdentity": immutable_identity})

        immutable_identity_list.append(partner_user.get("immutableIdentity"))
        target_partner_user_list.append(partner_user)

        # 先根据partnerId，userId查询,因为是删除，就不用查之前软删过的了
    local_partner_user_list = session.query(PartnerUserModel).filter(
            and_(
                    PartnerUserModel.deleted_at.is_(None),
                    PartnerUserModel.immutable_identity.in_(immutable_identity_list),
                    )
            ).all()
    platform_user_id_list = []
    for local_partner_user in local_partner_user_list:
        platform_user_id_list.append(local_partner_user.platform_user_id)
    local_gaia_user_list = session.query(GaiaUserModel).filter(
            and_(
                    GaiaUserModel.deleted_at.is_(None),
                    GaiaUserModel.platform_user_id.in_(platform_user_id_list)
                    )
            ).all()
    local_gaia_user_map = {}
    for local_gaia_user in local_gaia_user_list:
        local_gaia_user_map.update({
            str(local_gaia_user.platform_user_id): local_gaia_user
            })

    for target_partner_user in target_partner_user_list:
        find = False
        for local_partner_user in local_partner_user_list:
            if target_partner_user.get("immutableIdentity") == local_partner_user.immutable_identity:
                find = True
                # local_partner_user.partner_user_id = target_partner_user.get("userId")
                local_partner_user.json_data = json.dumps(target_partner_user)
                local_partner_user.update_required = True
                local_gaia_user = local_gaia_user_map.get(local_partner_user.platform_user_id)
                local_gaia_user.json_data = json.dumps(target_partner_user)
                # 修改update_required
                local_partner_user.update_required = False
                local_gaia_user.update_required = True
    session.flush()

    for local_partner_user in local_partner_user_list:
        process_login_details(local_partner_user.platform_user_id, session)
        new_local_gaia_user = session.query(GaiaUserModel).filter(
                and_(
                        GaiaUserModel.deleted_at.is_(None),
                        GaiaUserModel.platform_user_id == local_partner_user.platform_user_id
                        )
                ).first()
        new_local_gaia_user.update_required = False

    session.commit()
