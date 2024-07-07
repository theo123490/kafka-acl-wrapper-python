import confluent_kafka.admin as kafka_admin
import os 

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

acl_binding_filter_all = kafka_admin.AclBindingFilter(
    principal=None,
    host=None,
    operation=kafka_admin.AclOperation.ANY,
    permission_type=kafka_admin.AclPermissionType.ANY,
    restype=kafka_admin.ResourceType.ANY,
    resource_pattern_type=kafka_admin.ResourcePatternType.ANY,
    name=None
)

acls = admin.describe_acls(acl_binding_filter_all).result()
print(acls)

