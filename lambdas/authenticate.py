import json

import requests

from layer.code import Code


def build_iam_policy(principal_id, effect, resource, context):
    """
    构建权限策略
    :param principal_id: 这里使用ssoId
    :param effect: Allow or Deny￿
    :param resource: 需要访问的函数资源 methodArn
    :param context: 传递到下一个函数的数据（ssoId、lang、appId、expired）
    """
    policy = {
        'principalId': principal_id,
        'policyDocument': {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Action': 'execute-api:Invoke',
                    'Effect': effect,
                    'Resource': resource,
                },
            ],
        },
        'context': context,
    }

    # 打印日志
    print("Authorization info:%s", json.dumps(policy))
    return policy


def lambda_handler(event, context):
    global effect, platform_user_id

    # 无效token
    authorizer_invalid_context = {
        'code': Code.TOKEN_INVALID.code,
        'message': Code.TOKEN_INVALID.message
    }
    authorizer_No_token_context = {
        'code': Code.TOKEN_NULL.code,
        'message': Code.TOKEN_NULL.message
    }
    authorizer_username_invalid_context = {
        'code': Code.USER_NOT_EXIST.code,
        'message': Code.USER_NOT_EXIST.message
    }

    authorizer_user_not_exist_context = {
        'code': Code.USER_NOT_EXIST.code,
        'message': Code.USER_NOT_EXIST.message
    }

    try:
        # 1.获取ssoToken
        union_token = event.get("authorizationToken")
        if union_token is None:
            return build_iam_policy('', 'Deny', event['methodArn'], authorizer_No_token_context)

        # user_name = event.get("User-Name")
        # 分割token 取user_name
        res = union_token.split("&", 1)
        user_name = res[1]
        sso_token = res[0]
        print("user_name", user_name)
        print("sso_token", sso_token)

        if sso_token is None:
            return build_iam_policy('', 'Deny', event['methodArn'], authorizer_No_token_context)
        if user_name is None:
            return build_iam_policy('', 'Deny', event['methodArn'], authorizer_username_invalid_context)

        # 注意lambda要从此处获取值
        # saas_access_token = event.get("authorizationToken")

        # snippet start :[ http请求getUserInfo]

        # authorizer_context = {
        #     'accountId': sso_token,
        #     # 'lang': lang,
        #     'platformUserId': "40000166",
        #     'accountSystemId': "sso"
        #     # 'clientId': client_id,
        #     # 'expired': expired
        # }

        # iam_policy = build_iam_policy("1212121", 'Allow', event['methodArn'], authorizer_context)
        # return iam_policy

        # sso测试环境
        url = "https://logintest.tclclouds.com/account/getuserinfo"
        # sso正式环境
        url = "https://login.tclclouds.com/account/getuserinfo"

        param = {
            "username": user_name,
            "token": sso_token
        }

        response = requests.get(url, params=param)
        content = json.loads(response.text)
        # snippet end :[ http请求getUserInfo]

        if content.get('status') == 1:
            # 成功返回
            # account_id = content.get('user').get('id')
            account_id = content.get('user').get('username')
            print("account--->:{}", account_id)
            # snippet start [请求platform 的userId]
            url = "http://52.80.46.135:8080/user/platformId/sso/" + str(account_id)
            param = {

            }
            response = requests.get(url, params=param)
            content = json.loads(response.text)
            print("请求platform 的userId 的content--->:", content)
            if content.get("status") != Code.SUCCESS.message:
                message = content.get("message")
                print('platform_user_id is None & message--->', message)
                return build_iam_policy('', 'Deny', event['methodArn'], authorizer_user_not_exist_context)
            else:
                platform_user_id = content.get("data").get("platform_user_id")
                authorizer_context = {
                    'accountId': account_id,
                    # 'lang': lang,
                    'platformUserId': platform_user_id,
                    'accountSystemId': "sso"
                    # 'clientId': client_id,
                    # 'expired': expired
                }
                #
                allowed = True
                effect = 'Allow' if allowed else 'Deny'

                # 5.创建权限策略
                resource = event['methodArn']
                # iam_policy = build_iam_policy(platform_user_id, effect, resource, authorizer_context)
                iam_policy = build_iam_policy(platform_user_id, effect, resource, authorizer_context)
                return iam_policy




        elif content.get('status') == 102:
            print("token 为空", content)
            # token 为空
            return build_iam_policy('', 'Deny', event['methodArn'], authorizer_No_token_context)
        elif content.get('status') == 101:
            #     invalid user_ name
            print("invalid user_name", content)
            return build_iam_policy('', 'Deny', event['methodArn'], authorizer_username_invalid_context)
        elif content.get('status') == 103:
            #     error token
            print("error token", content)
            return build_iam_policy('', 'Deny', event['methodArn'], authorizer_invalid_context)
        else:
            #     invalid user_ name
            print("用户不存在", content)
            return build_iam_policy('', 'Deny', event['methodArn'], authorizer_user_not_exist_context)

        # if platform_user_id is None:
        #     print('platform_user_id is None')
        #     return build_iam_policy(platform_user_id, 'Deny', event['methodArn'], authorizer_invalid_context)
        # if not client_id:
        #     print('clientId is None')
        #     return build_iam_policy(account_id, 'Deny', event['methodArn'], authorizer_invalid_context)

        # principal_id = account_id
        # effect = 'Deny'
        # resource = event.get("methodArn")
        # authorizer_wrong_context = {
        #     'code': 'Wrong token',
        #     'message': 'Wrong token /DynamoDb not found token'
        # }
        #
        # if saas_access_token != access_token_rsp:
        #     iam_policy = build_iam_policy(principal_id, effect, resource, authorizer_wrong_context)
        #     return iam_policy

        #
        # 4.context将会传递给下一个lambda


    except Exception as e:
        print(e)
        allowed = False
        principal_id = ''
        resource = event['methodArn']
        authorizer_context = authorizer_invalid_context
        print("Error when doing authorize sso_token")
        return build_iam_policy(principal_id, 'Deny', resource, authorizer_context)


# 测试
# class MyTest():
#     """
#     用于
#     """
#     event = {
#         "authorizationToken": "CN_dc061b8db40e4d6b5abc455f7f14f191&177923673"
#         ,
#         # "User-Name": ""
#         # ,
#         "methodArn": "arn:aws:lambda:ap-southeast-1:322919161366:function:IoT-Platform-CloudPoc-AuthSaasAuthorizer-1IBS6E49VVA65"
#     }
#     context = ''
#
#     res = lambda_handler(event, context)
#     print("最终返回值--->:", res)
#
#
# if __name__ == "__main__":
#     MyTest()

    # EOF
