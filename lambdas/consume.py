from layer import Layer, Env
from lambdas.user_event.user_added import user_added_data
from lambdas.user_event.user_deleted1 import user_deleted_data
from lambdas.user_device_event.user_device_binding import user_device_binding_data
from lambdas.user_event.user_updated import user_update_data

from devicemanagement.device_cosume import consume_device_event


def consume(data, l: Layer):
    print("data type 是--->:{},data 是{}".format(type(data), data))
    # UserAdded.event
    # ''' [{'userId': '2030836', 'immutableIdentity': 'tcl-sso:', 'userName': 'Ethan', 'mobile': '13120575591',
    #    'email': 'yuyoung@613.com', 'login_details': [{'accountSystemId': 'tcl-sso', 'loginAccountId': '23222233'}],
    #    'tenantId': '', 'idType': 'passport', 'idNo': '2121212111', 'partnerId': 'china_iot',
    #    'messageType': 'UserAdded.command'}]'''

    # UserDeleted.event
    # [{'userId': '2030838', 'partnerId': 'china_iot', 'messageType': 'UserDeleted.event'}, {'userId': '2030839', 'partnerId': 'china_iot', 'messageType': 'UserDeleted.event'}]

    # UserDeviceBinding.Event
    # [{'deviceId': '2030838', 'userId': '2030837', 'userRole': '1', 'partnerId': 'china_iot', 'messageType': 'UserDeviceBinding.Event'}]
    session = l.session()

    try:
        assert isinstance(data, list)
        # 新增
        if data[0]["messageType"] == "UserAdded.Event":
            user_added_data(data, session)
        elif data[0]["messageType"] == "UserDeleted.Event":
            user_deleted_data(data, session)
        elif data[0]["messageType"] == "UserUpdated.Event":
            user_update_data(data, session)
        elif data[0]["messageType"] == "UserDeviceBinding.Event":
            user_device_binding_data(data, session)
        else:
            # 处理device的事件
            consume_device_event(data, l)
            # print("不是要处理的event的类型，数据为{}".format(data))
            # pass

    except Exception as e:
        print(e)
        session.rollback()
    finally:
        session.close()


if __name__ == '__main__':
    l = Layer(Env.DEV)
    user_added_event = [
        {'userId': '400002', 'userName': 'Ethan', 'mobile': '13120575592',
         'immutable_identity': 'tcl-sso:sso-123456',
         'email': 'yuyoung@613.com',
         'login_details': [{'accountSystemId': 'tcl-sso', 'loginAccountId': 'sso-44444'}],
         'tenantId': 'TCL', 'partnerId': 'ciot',
         'messageType': 'UserAdded.Event'}]

    user_deleted_event = [{'userId': '2030841', 'partnerId': 'china_iot', 'messageType': 'UserDeleted.Event'},
                          # {'userId': '2030839', 'partnerId': 'china_iot', 'messageType': 'UserDeleted.event'}
                          ]
    user_updated_event = [
        {'userId': '2030841', 'userName': 'Ethan3', 'mobile': '13120575592',
         'email': 'yuyoung@613.com',
         'login_details': [{'accountSystemId': 'tcl-sso', 'loginAccountId': 'sso-2030841'}],
         'tenantId': 'TCL', 'partnerId': 'china_iot',
         'messageType': 'UserUpdated.Event'}]

    user_device_binding_event = [
        {'deviceId': '2030838', 'userId': '2030841', 'userRole': '1', 'partnerId': 'oversea_iot',
         'messageType': 'UserDeviceBinding.Event'}]
    consume(user_device_binding_event, l)
