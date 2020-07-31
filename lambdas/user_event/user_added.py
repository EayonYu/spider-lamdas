import datetime
import json
import uuid

from layer.model import GaiaUserModel
from layer.model import PartnerUserModel
from sqlalchemy import and_

from lambdas.user_event.user_deleted1 import process_login_details

'''
    先看有没有 immutable_identity
        没有则拼装个 immutable_identity
    用 immutable_identity 查partner user 表
        如果存在
            是软删
                判断 partner_user_id 是否对应的上，对应的上
                    更改其他信息
                    软删则恢复(新增)
                    partner user updated_required = True

                    新增 Gaia User 
                    取返回的 id 
                    partner user updated_required = False 
                    gaia user updated_required = True 
                    process_login_details(platfrom_user_id,session)
                    gaia user updated_required = False 
                    
                    
                对应不上
                    raise error(已被其他用户注册过)  
                
            不是软删
                immutable_identity 已经存在
                判断 partner_user_id 是否对应的上，对应的上
                    log (已有存在用户正在使用，此次不做更改)
                    pass
                对应不上
                    raise error
        不存在
            # 新增
            新增partner user 
            
            partner user updated_required = True
            新增 Gaia User 
            取返回的 id 
            partner user updated_required = False 
            gaia user updated_required = True 
            process_login_details(platfrom_user_id,session)
            gaia user updated_required = False 
                    
            
'''


def add_gaia_user(partner_user, session):
    # 用 immutable_identity 查询 gaia_user 中的用户
    local_gaia_user = session.query(GaiaUserModel).filter(
            and_(
                    GaiaUserModel.immutable_identity == partner_user["immutable_identity"]
                    )
            ).first()

    if not local_gaia_user:
        # 没有exist gaia user
        gaia_user_model = GaiaUserModel()
        # 先用uuid 后期可以变为已经设计定义的id
        gaia_user_model.platform_user_id = str(uuid.uuid1())
        gaia_user_model.immutable_identity = partner_user["immutable_identity"]
        gaia_user_model.update_required = True
        gaia_user_model.created_at = datetime.datetime.utcnow()
        gaia_user_model.json_data = json.dumps(partner_user)
        session.add(gaia_user_model)
        session.flush()
        platform_user_id = gaia_user_model.id
        gaia_user_model.platform_user_id = platform_user_id
        session.flush()



    else:
        # merge user
        # 有已经存在的 gaia user  (此处就是合并用户)
        platform_user_id = local_gaia_user.id
        # 修改信息
        local_gaia_user.updated_at = datetime.datetime.utcnow()
        local_gaia_user.json_data = json.dumps(partner_user)
        local_gaia_user.update_required = True

        if local_gaia_user.deleted_at:
            # 恢复
            local_gaia_user.deleted_at = None
    return platform_user_id


def add_all_process(partner_user, session):
    target_partner_user = PartnerUserModel()
    target_partner_user.created_at = datetime.datetime.utcnow()
    target_partner_user.partner_user_id = partner_user["userId"]
    target_partner_user.partner_id = partner_user["partnerId"]
    target_partner_user.update_required = True
    target_partner_user.json_data = json.dumps(partner_user)
    if "china_iot" == partner_user["partnerId"]:
        # partnerId 在close partner 的集合中
        target_partner_user.mapping_mode = "AUTOMATIC"
    target_partner_user.immutable_identity = partner_user["immutable_identity"]
    session.add(target_partner_user)
    platform_user_id = add_gaia_user(partner_user, session)
    target_partner_user.platform_user_id = platform_user_id
    target_partner_user.update_required = False

    process_login_details(platform_user_id, session)
    new_local_gaia_user = session.query(GaiaUserModel).filter(
            and_(
                    GaiaUserModel.deleted_at.is_(None),
                    GaiaUserModel.platform_user_id == platform_user_id
                    )
            ).first()
    new_local_gaia_user.update_required = False


def user_added_data(data: list, session):
    # 新增
    for partner_user in data:
        if not partner_user.get("immutable_identity"):
            partner_user.update({
                "immutable_identity": str(partner_user["partnerId"] + ":" + str(partner_user["userId"]))
                })
        local_partner_user_include_deleted_list = session.query(PartnerUserModel).filter(
                and_(
                        PartnerUserModel.immutable_identity == partner_user["immutable_identity"]
                        )
                ).all()
        if local_partner_user_include_deleted_list:
            find = False
            for local_partner_user_include_deleted in local_partner_user_include_deleted_list:

                # if local_partner_user_include_deleted:
                if local_partner_user_include_deleted.deleted_at:
                    #     是软删
                    if partner_user["userId"] == local_partner_user_include_deleted.partner_user_id and \
                            partner_user["partnerId"] == local_partner_user_include_deleted.partner_id:
                        find = True
                        local_partner_user_include_deleted.updated_required = True
                        local_partner_user_include_deleted.json_data = json.dumps(partner_user)
                        if "china_iot" == partner_user["partnerId"]:
                            # partnerId 在close partner 的集合中
                            local_partner_user_include_deleted.mapping_mode = "AUTOMATIC"
                        local_partner_user_include_deleted.deleted_at = None
                        # 新增gaia_user
                        platform_user_id = add_gaia_user(partner_user, session)
                        local_partner_user_include_deleted.platform_user_id = platform_user_id
                        local_partner_user_include_deleted.update_required = False
                        # 新增gaia_user end
                        # 处理 login_details
                        process_login_details(platform_user_id, session)
                        new_local_gaia_user = session.query(GaiaUserModel).filter(
                                and_(
                                        GaiaUserModel.deleted_at.is_(None),
                                        GaiaUserModel.platform_user_id == platform_user_id
                                        )
                                ).first()
                        new_local_gaia_user.update_required = False
                        # 处理 login_details end
                    elif partner_user["userId"] != local_partner_user_include_deleted.partner_user_id and \
                            partner_user["partnerId"] == local_partner_user_include_deleted.partner_id:
                        # 对应不上(同一个partner系统,immutable_identity 不允许对应两个用户)
                        raise Exception("{} 已被其他用户注册过".format(partner_user["immutable_identity"]))
                    # else:
                    #     # 其他partner system 但是同一个登录用户，并且是被删除过的
                    #     add_all_process(partner_user, session)
                else:
                    # 非软删
                    if partner_user["userId"] == local_partner_user_include_deleted.partner_user_id and \
                            partner_user["partnerId"] == local_partner_user_include_deleted.partner_id:
                        find = True
                        print("已有存在用户正在使用,此次不做更改")
                    elif partner_user["userId"] != local_partner_user_include_deleted.partner_user_id and \
                            partner_user["partnerId"] == local_partner_user_include_deleted.partner_id:
                        # 对应不上(同一个partner系统,immutable_identity 不允许对应两个用户)
                        raise Exception("{} 已被其他用户注册过".format(partner_user["immutable_identity"]))
                    # else:
                    #     # 其他partner system 但是同一个登录用户，并且是被删除过的
                    #     add_all_process(partner_user, session)
            if not find:
                add_all_process(partner_user, session)

        else:
            # 不存在
            # 新增partner user

            #             partner user updated_required = True
            #             新增 Gaia User
            #             取返回的 id
            #             partner user updated_required = False
            #             gaia user updated_required = True
            #             process_login_details(platfrom_user_id,session)
            #             gaia user updated_required = False
            add_all_process(partner_user, session)

    session.commit()

    # # 新增
    # for partner_user in data:
    #     # 先根据partnerId，userId查询
    #     local_partner_user_include_deleted = session.query(PartnerUserModel).filter(
    #             and_(
    #                     PartnerUserModel.partner_id == partner_user["partnerId"],
    #                     PartnerUserModel.partner_user_id == partner_user["userId"]
    #                     )
    #             ).first()
    #     target_immutable_identity = partner_user.immutable_identity
    #     if not target_immutable_identity:
    #         target_immutable_identity = partner_user["partnerId"] + ":" + partner_user["userId"]
    #
    #     if local_partner_user_include_deleted:
    #         if local_partner_user_include_deleted.immutable_identity == target_immutable_identity:
    #             # 是同一本人账户并且之前删除过
    #             if local_partner_user_include_deleted.deleted_at:
    #                 # 恢复
    #                 local_partner_user_include_deleted.deleted_at = None
    #                 # todo
    #             else:
    #                 raise Exception("此用户之前已同步过到gaia平台并且使用的不是此 immutable_identity: {}".format(target_immutable_identity))
    #         else:
    #             print("immutable_identity不匹配已有或已删除账户")
    #     else:
    #         #  之前没有该partner用户，在根据 immutable_identity 去查询
    #         local_exist_immutable_identity_user = session.query(PartnerUserModel).filter(
    #                 and_(
    #                         PartnerUserModel.deleted_at.is_(None),
    #                         PartnerUserModel.immutable_identity == target_immutable_identity
    #                         )
    #                 ).first()
    #         if local_exist_immutable_identity_user:
    #             raise Exception("此immutable_identity: {} 已经被同一partner系统的其他用户使用过".format(target_immutable_identity))
    #         else:
    #             # 没有被使用,开始新增partner_user
    #             partner_user_model = PartnerUserModel()
    #             partner_user_model.partner_id = partner_user["partnerId"]
    #             partner_user_model.partner_user_id = partner_user["userId"]
    #             partner_user_model.update_required = True
    #             partner_user_model.json_data = json.dumps(partner_user)
    #
    #             if "china_iot" == partner_user["partnerId"]:
    #                 # partnerId 在close partner 的集合中
    #                 partner_user_model.mapping_mode = "AUTOMATIC"
    #             partner_user_model.immutable_identity = target_immutable_identity
    #             session.add(partner_user_model)
    #         # 用 immutable_identity 查询 gaia_user 中的用户
    #         local_gaia_user = session.query(GaiaUserModel).filter(
    #                 and_(
    #                         GaiaUserModel.immutable_identity == target_immutable_identity
    #                         )
    #                 ).first()
    #
    #         if not local_gaia_user:
    #             # 没有exist gaia user
    #             gaia_user_model = GaiaUserModel()
    #             # 先用uuid 后期可以变为已经设计定义的id
    #             gaia_user_model.platform_user_id = str(uuid.uuid1())
    #             gaia_user_model.immutable_identity = target_immutable_identity
    #             gaia_user_model.update_required = True
    #             session.add(gaia_user_model)
    #             # add gaia login detail
    #             session.flush()
    #             platform_user_id = gaia_user_model.id
    #
    #             # 新增 GaiaLoginDetail list
    #             for tmp_login_detail in partner_user["login_details"]:
    #                 gaia_login_detail = GaiaLoginDetailModel()
    #                 gaia_login_detail.platform_user_id = platform_user_id
    #                 gaia_login_detail.account_system_id = tmp_login_detail["accountSystemId"]
    #                 gaia_login_detail.login_account_id = tmp_login_detail["loginAccountId"]
    #                 session.add(gaia_login_detail)
    #         else:
    #             platform_user_id = local_gaia_user.id
    #             # 有已经存在的 gaia user 则新增或者修改login details
    #             if local_gaia_user.deleted_at:
    #                 # 恢复
    #                 local_gaia_user.deleted_at = None
    #
    #                 # 处理login details
    #                 target_login_details = []
    #
    #                 for tmp_login_detail in partner_user["login_details"]:
    #                     gaia_login_detail = GaiaLoginDetailModel()
    #                     gaia_login_detail.platform_user_id = platform_user_id
    #                     gaia_login_detail.account_system_id = tmp_login_detail["accountSystemId"]
    #                     gaia_login_detail.login_account_id = tmp_login_detail["loginAccountId"]
    #                     gaia_login_detail.deleted_at = None
    #                     target_login_details.append(gaia_login_detail)
    #                 local_login_details_list = session.query(GaiaLoginDetailModel).filter(
    #                         and_(
    #                                 GaiaLoginDetailModel.deleted_at.is_(None),
    #                                 GaiaLoginDetailModel.platform_user_id == platform_user_id
    #                                 )
    #                         ).all
    #                 wait_to_add = []
    #                 if local_login_details_list:
    #
    #                     for target_login_detail in target_login_details:
    #
    #                         for local_login_detail in local_login_details_list:
    #                             if (target_login_detail.account_system_id == local_login_detail.account_system_id and
    #                                     target_login_detail.login_account_id == local_login_detail.login_account_id
    #                             ):
    #                                 # 找到
    #                                 pass
    #                             else:
    #                                 # 没有找到，新增
    #                                 wait_to_add.append(target_login_detail)
    #                                 session.add_all(wait_to_add)
    #             else:
    #                 for tmp_login_detail in partner_user["login_details"]:
    #                     gaia_login_detail = GaiaLoginDetailModel()
    #                     gaia_login_detail.platform_user_id = platform_user_id
    #                     gaia_login_detail.account_system_id = tmp_login_detail["accountSystemId"]
    #                     gaia_login_detail.login_account_id = tmp_login_detail["loginAccountId"]
    #                     gaia_login_detail.deleted_at = None
    #                     one = session.query(GaiaLoginDetailModel).filter(and_(
    #                             GaiaLoginDetailModel.deleted_at.is_(None),
    #                             GaiaLoginDetailModel.account_system_id == gaia_login_detail.account_system_id,
    #                             GaiaLoginDetailModel.login_account_id == gaia_login_detail.login_account_id
    #                             )).first()
    #                     if one:
    #                         pass
    #                     else:
    #                         session.add(gaia_login_detail)
    #     session.commit()
