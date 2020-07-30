class ImmutableIdentityElement:
    def __init__(self):
        self.manufacturer = None
        self.product_type = None
        self.sn = None
        self.mac = None
        self.partner_id = None
        self.partner_device_id = None


def generate_immutable_identity(immutable_identity_element: ImmutableIdentityElement):
    manufacturer = immutable_identity_element.manufacturer
    product_type = immutable_identity_element.product_type
    sn = immutable_identity_element.sn
    mac = immutable_identity_element.mac
    partner_id = immutable_identity_element.partner_id
    partner_device_id = immutable_identity_element.partner_device_id

    if manufacturer and product_type:
        if sn:
            return f'manufacturer:{manufacturer};product-type:{product_type};SN:{sn}'
        elif mac:
            return f'manufacturer:{manufacturer};product-type:{product_type};MAC:{mac}'
    elif partner_id and partner_device_id:
        return f'partner-id:{partner_id};partner-device-id:{partner_device_id}'
    else:
        pass
