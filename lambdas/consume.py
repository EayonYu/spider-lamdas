
from layer import Layer, Env
from layer.model import PartnerUserModel


def consume(data, l: Layer):
    session = l.session()

    try:
        partner_user = PartnerUserModel()
        partner_user.partner_id = '1'
        partner_user.partner_user_id = '2'
        partner_user.json_data = '{"hello":"world"}'
        partner_user.immutable_identity = data
        session.add(partner_user)
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()
    finally:
        session.close()

    print(data)


if __name__ == '__main__':
    l = Layer(Env.DEV)
    consume("hello", l)
