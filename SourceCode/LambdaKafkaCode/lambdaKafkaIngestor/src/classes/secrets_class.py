from src.errors.lambda_error import LambdaError
import boto3
from botocore.exceptions import ClientError


class SecretsManagement():
    """Manage secrets manager interactions."""

    def __init__(self, config, logger):
        """
        Constructor method. handles all config properties for class.
        :param config: config properties from config file
        :param logger: object to send logs to clowdwatch logs.
        """
        self.logger = logger
        self.config = config
        self.tmp_dir = config['lambda']['temp_folder']
        try:
            self.secrets_client = boto3.client('secretsmanager'
                                               , region_name=config['aws_region_name'])
        except (ClientError, KeyError) as error:
            raise LambdaError(
                f"Failed on [secrets_class][__init__]. Detail: {error}")

    def create_certificates_from_secrets(self):
        """
        read config properties related to secret names where SSL Certificates were configured,
        read secrets and writes certificates/ private/ ca keys temporary.
        :return: updated config object with certificates/keys/ca temp path.
        """
        secrets_cert_location = self.config['kafka']["secrets_ssl_certificate_location"]
        certificate_content = self.get_secret_value(secrets_cert_location)
        self.write_content_to_tmp_file(certificate_content, "certificate.pem")
        self.config['kafka']["ssl_certificate_location"] = self.tmp_dir + "certificate.pem"

        secrets_key_location = self.config['kafka']["secrets_ssl_key_location"]
        key_content = self.get_secret_value(secrets_key_location)
        self.write_content_to_tmp_file(key_content, "key.pem")
        self.config['kafka']["ssl_key_location"] = self.tmp_dir + "key.pem"

        secrets_trustore_location = self.config['kafka'].get("secrets_ssl_ca_location")
        if secrets_trustore_location:
            trustore_content = self.get_secret_value(secrets_trustore_location)
            self.write_content_to_tmp_file(trustore_content, "all.pem")
            self.config['kafka']["ssl_ca_location"] = self.tmp_dir + "all.pem"

        return self.config

    def get_secret_value(self, secret_stored_location):
        """
        retrieve a secret content from a given secret name.
        :param secret_stored_location: secret name
        :return: secret content
        """
        try:
            response = self.secrets_client.get_secret_value(SecretId=secret_stored_location)
            return response['SecretString']

        except (ClientError, KeyError) as error:
            raise LambdaError("Failed on [secrets_class][get_secret_value]."
                              f"\nDetail: {error}")

    def write_content_to_tmp_file(self, content, file_name):
        """
        write a string content to a local file given the local file name.
        :param content : string with the content to write
        :param content : local file name in the lambda context
        """
        with open(self.tmp_dir + file_name, "w") as text_file:
            text_file.write(content)
