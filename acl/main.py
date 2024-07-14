import confluent_kafka.admin as kafka_admin
from pprint import pprint
import os 

def main():
    admin = setup_kafka_admin_client()
    acls = get_all_acl_bindings(admin)
    pprint(acls)

def get_all_acl_bindings(admin):
    acl_binding_filter = create_acl_binding_filter()
    acls = get_acl(admin, acl_binding_filter)
    return acls

def get_producer_acls(admin, principal, topic):
    acl_bindings = create_producer_acl_binding(principal, topic)
    acls = acl_bindings_to_filter(admin, acl_bindings)
    return acls

def get_consumer_acls(admin, principal, topic, group):
    acl_bindings = create_consumer_acl_binding(principal, topic, group)
    acls = acl_bindings_to_filter(admin, acl_bindings)
    return acls

def acl_bindings_to_filter(admin, acl_bindings):
    acls = []
    for binding in acl_bindings:
        acl_binding_filter = kafka_admin.AclBindingFilter(
                                principal = binding.principal,
                                host = binding.host,
                                name = binding.name,
                                operation = binding.operation,
                                permission_type = binding.permission_type,
                                restype = binding.restype,
                                resource_pattern_type = binding.resource_pattern_type
                            )
        acl = get_acl(admin,acl_binding_filter)
        acls.append(acl)
    return acls

def get_acl(admin,acl_binding_filter):
    acls = admin.describe_acls(acl_binding_filter).result()
    return acls

def create_acl_topic_consumer(admin, principal, topic, group=None):
    acl_bindings = create_consumer_acl_binding(principal, topic, group)
    admin.create_acls(acl_bindings)

def create_consumer_acl_binding(principal, topic, group):
    acl_bindings = []
    if group != None:
        acl_binding_group = kafka_admin.AclBinding( 
                                restype = kafka_admin.ResourceType.GROUP,
                                name = group,
                                resource_pattern_type = kafka_admin.ResourcePatternType.LITERAL,
                                principal = principal,
                                host = '*',
                                operation = kafka_admin.AclOperation.READ,
                                permission_type = kafka_admin.AclPermissionType.ALLOW
                            ) 
        acl_bindings.append(acl_binding_group)

    acl_binding_read = kafka_admin.AclBinding( 
        restype = kafka_admin.ResourceType.TOPIC,
        name = topic,
        resource_pattern_type = kafka_admin.ResourcePatternType.LITERAL,
        principal = principal,
        host = '*',
        operation = kafka_admin.AclOperation.READ,
        permission_type = kafka_admin.AclPermissionType.ALLOW
    )    
    acl_binding_describe = kafka_admin.AclBinding( 
        restype = kafka_admin.ResourceType.TOPIC,
        name = topic,
        resource_pattern_type = kafka_admin.ResourcePatternType.LITERAL,
        principal = principal,
        host = '*',
        operation = kafka_admin.AclOperation.DESCRIBE,
        permission_type = kafka_admin.AclPermissionType.ALLOW
    )    
    acl_bindings.append(acl_binding_read)
    acl_bindings.append(acl_binding_describe)

    return acl_bindings


def create_acl_topic_producer(admin, principal, topic):
    acl_bindings = create_producer_acl_binding(principal, topic)
    admin.create_acls(acl_bindings)    

def create_producer_acl_binding(principal, topic):
    acl_bindings = []
    acl_binding_write =  kafka_admin.AclBinding( 
                            restype = kafka_admin.ResourceType.TOPIC,
                            name = topic,
                            resource_pattern_type = kafka_admin.ResourcePatternType.LITERAL,
                            principal = principal,
                            host = '*',
                            operation = kafka_admin.AclOperation.WRITE,
                            permission_type = kafka_admin.AclPermissionType.ALLOW
                            )    
    acl_binding_describe =  kafka_admin.AclBinding( 
                            restype = kafka_admin.ResourceType.TOPIC,
                            name = topic,
                            resource_pattern_type = kafka_admin.ResourcePatternType.LITERAL,
                            principal = principal,
                            host = '*',
                            operation = kafka_admin.AclOperation.DESCRIBE,
                            permission_type = kafka_admin.AclPermissionType.ALLOW
                            )    
    acl_binding_create =  kafka_admin.AclBinding( 
                            restype = kafka_admin.ResourceType.TOPIC,
                            name = topic,
                            resource_pattern_type = kafka_admin.ResourcePatternType.LITERAL,
                            principal = principal,
                            host = '*',
                            operation = kafka_admin.AclOperation.CREATE,
                            permission_type = kafka_admin.AclPermissionType.ALLOW
                            )
    acl_bindings.append(acl_binding_write)
    acl_bindings.append(acl_binding_describe)
    acl_bindings.append(acl_binding_create)

    return acl_bindings

    
def setup_kafka_admin_client():
    admin = kafka_admin.AdminClient ({
        'bootstrap.servers': os.environ.get('BOOTSTRAP_SERVER'),
        'ssl.keystore.location' : os.environ.get('SSL_KEYSTORE_LOCATION'),
        'ssl.keystore.password' : os.environ.get('SSL_KEYSTORE_PASSWORD'),
        'ssl.key.password' : os.environ.get('SSL_KEY_PASSWORD'),
        'security.protocol' : os.environ.get('SECURITY_PROTOCOL'),
        'ssl.providers' : os.environ.get('SSL_PROVIDERS'),
        'ssl.ca.location' : os.environ.get('SSL_CA_LOCATION'),
        'ssl.endpoint.identification.algorithm' : os.environ.get('SSL_ENDPOINT_IDENTIFICATION_ALGORITHM'),
        })

    return admin

def create_acl_binding_filter(
        principal = None,
        host = None,
        name = None,
        operation=kafka_admin.AclOperation.ANY,
        permission_type=kafka_admin.AclPermissionType.ANY,
        restype=kafka_admin.ResourceType.ANY,
        resource_pattern_type=kafka_admin.ResourcePatternType.ANY):

    acl_binding_filter = kafka_admin.AclBindingFilter(
        principal=principal,
        host=host,
        operation=operation,
        permission_type=permission_type,
        restype=restype,
        resource_pattern_type=resource_pattern_type,
        name=name
    )
    return acl_binding_filter

if __name__ == "__main__":
    main()
