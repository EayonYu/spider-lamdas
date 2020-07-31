import datetime
from functools import reduce

from layer.model import GaiaLoginDetailModel
from layer.model import GaiaUserModel
from layer.model import PartnerUserModel
from sqlalchemy import and_
import json


def list_dict_duplicate_removal(data_list):
    run_function = lambda x, y: x if y in x else x + [y]
    return reduce(run_function, [[], ] + data_list)


def delete_duplicate(li):
    temp_list = list(set([str(i) for i in li]))
    li = [eval(i) for i in temp_list]
    return li


def process_login_details(platform_user_id: str, session):
    """
    同步login details 包含了增、删、改
    :param platform_user_id:
    :type platform_user_id:
    :param session:
    :type session:
    :return:
    :rtype:
    """
    # 组合 local_partner_login_details
    local_exist_partner_user_list = session.query(PartnerUserModel).filter(
            and_(
                    PartnerUserModel.deleted_at.is_(None),
                    PartnerUserModel.platform_user_id == platform_user_id
                    )
            ).all()
    # 用来存放目标login_details
    tmp_target_login_detail_list = []
    for item in local_exist_partner_user_list:
        json_data = json.loads(item.json_data)
        login_details = json_data.get("login_details")
        tmp_target_login_detail_list.extend(login_details)

    # 去重 login_details_list
    # target_login_detail_reduplicate_list = []
    # for target_login_detail in tmp_target_login_detail_list:
    #     for target_login_detail_reduplicate in target_login_detail_reduplicate_list:
    #         if target_login_detail_reduplicate.get("accountSystemId") == target_login_detail.get("accountSystemId") and \
    #                 target_login_detail_reduplicate.get("loginAccountId") == target_login_detail.get("loginAccountId"):
    #             pass
    #         else:
    #             target_login_detail_reduplicate_list.append(target_login_detail)

    target_login_detail_reduplicate_list = delete_duplicate(tmp_target_login_detail_list)

    target_login_detail_list = target_login_detail_reduplicate_list
    current_login_detail_list = session.query(GaiaLoginDetailModel).filter(
            and_(
                    GaiaLoginDetailModel.platform_user_id == platform_user_id
                    )
            ).all()

    for target_login_detail in target_login_detail_list:
        find = False
        for current_login_detail in current_login_detail_list:
            if current_login_detail.account_system_id == target_login_detail.get("accountSystemId") and \
                    current_login_detail.login_account_id == target_login_detail.get("loginAccountId"):
                find = True
                # 找到后修改内容
                if current_login_detail.platform_user_id != platform_user_id:
                    current_login_detail.platform_user_id = platform_user_id
                if current_login_detail.deleted_at:
                    # 之前是软删除，则恢复（新增）
                    current_login_detail.deleted_at = None

                break
        if not find:
            # 新增
            wait_to_create_login_detail = GaiaLoginDetailModel()
            wait_to_create_login_detail.account_system_id = target_login_detail["accountSystemId"]
            wait_to_create_login_detail.login_account_id = target_login_detail["loginAccountId"]
            wait_to_create_login_detail.platform_user_id = platform_user_id
            wait_to_create_login_detail.created_at = datetime.datetime.utcnow()
            session.add(wait_to_create_login_detail)
    # 哪些需要删除
    for current_login_detail in current_login_detail_list:
        find = False
        for target_login_detail in list(target_login_detail_list):
            if current_login_detail.account_system_id == target_login_detail.get("accountSystemId") and \
                    current_login_detail.login_account_id == target_login_detail.get("loginAccountId"):
                find = True
                break
        if not find:
            # 删除操作
            current_login_detail.deleted_at = datetime.datetime.utcnow()
    session.flush()


def user_deleted_data(data: list, session):
    """
    删除partner user
    :param data:
    :type data:
    :param session:
    :type session:
    :return:
    :rtype:
    """
    #     删除
    for partner_user in data:

        # 先根据partnerId，userId查询,因为是删除，就不用查之前软删过的了
        local_partner_user = session.query(PartnerUserModel).filter(
                and_(
                        PartnerUserModel.deleted_at.is_(None),
                        PartnerUserModel.partner_id == partner_user["partnerId"],
                        PartnerUserModel.partner_user_id == partner_user["userId"]
                        )
                ).first()
        if local_partner_user:
            # 有要删除的数据

            # 提取platform_user_id
            platform_user_id = local_partner_user.platform_user_id

            local_partner_user.deleted_at = datetime.datetime.utcnow()
            local_partner_user.update_required = True
            if platform_user_id:
                # 解绑
                local_partner_user.platform_user_id = None

                # snippet  [设置 update_required] start
                first = session.query(GaiaUserModel).filter(
                        and_(
                                GaiaUserModel.deleted_at.is_(None),
                                GaiaUserModel.platform_user_id == platform_user_id
                                )
                        ).first()
                if first:
                    first.update_required = True
                # snippet  [设置 update_required] end

                local_partner_user.update_required = False
                session.flush()
                # 同步login details
                process_login_details(str(platform_user_id), session)
                # 取消标记
                if first:
                    first.update_required = False
            session.commit()
        else:
            print("没有要删除的数据")
            pass
