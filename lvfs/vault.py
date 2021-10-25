""" Basic Frontend for Hashicorp Vault """
from typing import Optional
import os
import time
import logging
import secrets
import threading
import hvac
import json
try:
    import keyring
except ImportError:
    keyring = None
try:
    import dotenv
    dotenv.load_dotenv()
except ImportError:
    logging.error(
        "python-dotenv is not installed. Ignoring any .env configuration files.")


class Vault:
    SINGLETON = None

    def __init__(self,
                 server=os.environ.get("VAULT_ENDPOINT"),
                 namespace=os.environ.get("VAULT_NAMESPACE"),
                 role_id=os.environ.get("VAULT_ROLE_ID"),
                 secret_id=os.environ.get("VAULT_SECRET_ID"),
                 username=os.environ.get("VAULT_USERNAME"),
                 password=os.environ.get("VAULT_PASSWORD")
                 ):
        """ Log into a specific vault using a role_id and secret_id or a username and password.

            Accepts
            -------
            server : str
                The URL endpoint of the Vault server.
                If unspecified, it will default to $VAULT_ENDPOINT.
                If neither are specified, it will raise an error.
            namespace : str
                The namespace of your vault, like "department/auth/team"
                If unspecified, it will default to $VAULT_NAMESPACE.
                If neither are specified, it will raise an error.
            role_id : str
                Your app role's role_id.
                It's like a username but it's a random generated hex code.
                If unspecified, it will default to $VAULT_ROLE_ID.
                If neither are specified, it will raise an error.
            secret_id : str
                Your app role's secret_id.
                It's like a password but it's a random generated hex code.
                If unspecified, it will default to $VAULT_SECRET_ID,
                and if that is also unspecified it will search your keyring.
                If none are specified, it will raise an error.
            username : str
                Your username to authenticate to Vault.
                In many cases you will use a role_id instead, but for interactive use,
                a username and password may be preferable. If available, the role_id
                and secret_id will be used instead.
            password : str
                Your password to authenticate to Vault.
                In many cases you will use a role_id instead, but for interactive use,
                a username and password may be preferable. If available, the role_id
                and secret_id will be used instead.


            Notes
            -----
            A role id and secret_id are not much different from a username and password.
            The role_id is okay to store in code, like a username would be,
            but the secret_id can't, because it's like a password.

            Vault doesn't help you store secret_id -  this is a case of catch 22.
            To break the cycle, you need something to store the secret_id.
            This class autodetects several methods of passing that secret.

            - Jenkins will store it for you when building, but if you save it in a Docker image,
              you have to treat the image as secret, which is inconvenient.
              If we find a VAULT_SECRET_ID environment variable, we will use it but not store it.
            - Kubernetes will store secrets for you as well, a good choice for services
            - Keyring will store secrets on interactive machines,
              like Mac, Windows, or GNOME or KDE on Linux. This doesn't include servers.
        """
        if not server:
            raise ValueError(
                "You must specify a Vault URL when using Vault integration in LVFS. "
                "You can do this by passing server=? when instantiating Vault, or "
                "you can provide it using the $VAULT_ENDPOINT environment variable."
            )
        if (role_id and not secret_id) or (secret_id and not role_id):
            raise ValueError(
                "You must specify both the role and secret ids or neither. "
                "Since they are also searched through the environment variables, "
                "this applies to environment variables as well. "
                "To default to using a secrets file, remove both $VAULT_ROLE_ID "
                "and $VAULT_SECRET_ID."
            )
        elif password and not username:
            raise ValueError(
                "You must specify both the username and password or neither. "
                "Since they are also searched through the environment variables, "
                "this applies to environment variables as well. "
                "To default to using a secrets file, remove both $VAULT_USERNAME "
                " and $VAULT_PASSWORD."
            )
        elif username and not password:
            if keyring:
                password = keyring.get_password("Vault", username)
            else:
                raise UserWarning(
                    "No support for system keyring has been found (import keyring failed). "
                )
            # The password may have been found in the keyring, or may not, even if it's installed.
            if not password:
                raise ValueError(
                    "A username was provided but a password was not to Vault.__init__, "
                    "and neither $VAULT_PASSWORD nor the system keyring have a password for "
                    "this purpose either. Username and password authentication are not supported "
                    "via vault.json because of security considerations. "
                    "As a result, autodetection has failed and LVFS Vault cannot authenticate."
                )
        elif role_id and username:
            raise ValueError(
                "There are too many credentials provided. Please provide both role_id and "
                "secret_id, or both username and password. Don't provide all four."
            )
        elif not (role_id or username):
            # No credentials were provided at all. Default to the file.
            if not os.path.exists("/etc/secrets/vault.json"):
                # There's no configuration at all for Vault. This is probably the most common
                # circumstance so we should be as helpful as possible. Users here probably
                # don't even know such a configuration exists until this error message.
                raise ValueError(
                    "No role_id or secret_id were provided by argument to Vault.__init__ "
                    "and no username and password were provided the same way, "
                    "and none of $VAULT_ROLE_ID, or $VAULT_SECRET_ID, or $VAULT_USERNAME, "
                    "or $VAULT_PASSWORD were populated in the environment. "
                    "Finally, even /etc/secrets/vault.json does not exist, "
                    "which would have contained an object with the keys "
                    "role_id, secret_id, and namespace. "
                    "So autodetection for authenticating to Vault failed. "
                    "And as a result LVFS URLs are unable to operate on remote targets. "
                    "Please provide credentials to get to the Vault one of the those ways, "
                    "(which means you will need to be provisioned a namespace in our vault) "
                    "or else handle credentials management yourself using an lvfs.yml "
                    "configuration file in one of the locations mentioned in the docstring for "
                    "lvfs.credentials.Credentials.init_register()"
                )
            with open("/etc/secrets/vault.json", "r") as credfile:
                creds = json.load(credfile)
                if "role_id" not in creds or "secret_id" not in creds:
                    raise ValueError(
                        "Invalid credentials in /etc/secrets/vault.json. "
                        "Both the role_id and secret_id keys must be provided. "
                        "Username and password authentication are not supported "
                        "from vault.json."
                    )
                role_id = creds["role_id"]
                secret_id = creds["secret_id"]
                if type(role_id) != str or type(secret_id) != str:
                    raise ValueError(
                        "role_id and secret_id in /etc/secrets/vault.json must be strings."
                    )
                # Namespace can be specified via an __init__ parameter or via the file.
                if not namespace and "namespace" not in creds:
                    raise ValueError(
                        "namespace was not provided to Vault.__init__ and it is also missing from "
                        "/etc/secrets/vault.json. Please specify it one of those ways."
                    )
                namespace = namespace or creds["namespace"]
                if type(namespace) != str:
                    raise ValueError(
                        "namespace as provided to Vault.__init__ or in /etc/secrets/vault.json"
                        "is not a string. It must be a string."
                    )

        # At this point the namespace may have been detected from configuration.
        # So now let's check for the namespace to see if it is valid.
        if not namespace:
            raise ValueError(
                "Namespace was not specified in Vault.__init__, "
                "and $VAULT_NAMESPACE was empty as well.")
        if type(namespace) != str:
            raise ValueError(
                "Namespace as provided to Vault.__init__ or in /etc/secrets/vault.json"
                "is not a string. It must be a string."
            )

        #
        # At this point:
        # - There must be both role_id and secret_id, or both username and password,
        #   and no other option.
        # - The namespace is a non-empty string.
        #

        self._client = hvac.Client(
            url=server,
            namespace=namespace,
            verify=False)
        if role_id:
            self._client.auth.approle.login(role_id, secret_id)
        else:
            self._client.auth.userpass.login(username, password)
        assert self._client.is_authenticated(), "Vault login failed"

        # Continuously refresh the vault tokens and data every 30 to 60 minutes.
        def refresh_daemon():
            while time.sleep(1800 + secrets.randbelow(1800)):
                self._client.renew_self_token()

        self._daemon = threading.Thread(target=refresh_daemon)

    def __getitem__(self, secret_name) -> dict:
        """ Get a secret from the vault by name.

            Secrets are a group of key-value pairs, not just one.
            So this function returns a dictionary, not a string.
            While in some cases transmitted or stored as JSON, the values are plain strings and
            they can't contain nested objects. If you need nesting in your secret's values, you
            will need to load and dump JSON instead.

            Accepts
            -------
            name: str/tuple: The name of the secret to load (multiple kv pairs) under the mount
                  point "secret", or a tuple containing the name of the secret and the mount point.
                  Examples: "my_secret", ("my_secret", "my_secret_engine")

            Examples
            --------
            As long as your secrets are only one level deep, you're good:
            ```
            from lvfs.vault import Vault
            creds = Vault.default()["ssh_jumpbox_credentials"]
            username = creds["username"]
            password = creds["password"]
            ```

            If you need a single string, you may need to make up a hierarchy instead.
            ```
            from lvfs.vault import Vault
            cert_as_text = Vault.default()["ssl_certificates"]["edge_node"]
            ```

            If you need more hierarchy, you can pack it up in json:
            ```
            import json
            from lvfs.vault import Vault
            admin_user_ids = json.dumps(Vault.default()["app_config"]["admin_user_ids"])
            ```
            """
        if type(secret_name) == str:
            return self._client.secrets.kv.v2.read_secret_version(
                path=secret_name,
                mount_point='secret'
            )["data"]["data"][secret_name]
        elif type(secret_name) == tuple:
            return self._client.secrets.kv.v2.read_secret_version(
                path=secret_name[0],
                mount_point=secret_name[1]
            )["data"]["data"]

    def create_or_update(self, secret_name, secret_content, mount_point='secret'):
        """ Create or update a secret in the Vault.

            Accepts
            -------
            - `secret_name`: str:    Name of the secret in question
            - `secret_content`: str: New contents to replace the existing secret with
            - `mount_point`: str:    The secrets engine to use. Defaults to 'secret'.

            Notes
            -----
            - This is a wrapper for the HVAC KV2 create_or_update_secret method.
            - Any keys not specified in secret_content will be *deleted*.
        """
        self._client.secrets.kv.v2.create_or_update_secret(
            path=secret_name,
            mount_point=mount_point,
            secret=secret_content
        )

    def patch(self, secret_name, secret_content):
        """ Update an existing secret in the vault. Fails if the secret does not exist.

            Accepts
            -------
            - `secret_name`: str: Name of the secret in question
            - `secret_content`: str: New contents to augment the existing secret with

            Notes
            -----
            - This is a wrapper for the HVAC KV2 patch method,
              which is itself a wrapper for the create_or_update_secret, using the cas argument.
            - Any keys not specified in secret_content will be left *unmodified*.
        """
        self._client.secrets.kv.v2.patch(
            path=secret_name,
            mount_point="secret",
            secret=secret_content
        )

    def delete(self, secret_name, mount_point='secret'):
        """ Completely obliterate a secret, including all version control.

            Accepts
            -------
            - `secret_name`: str: Name of the secret to vaporize
            - `mount_point`: str:    The secrets engine to use. Defaults to 'secret'.

            Notes
            -----
            - This is a wrapper for the HVAC KV2 delete_metadata_and_all_versions method
            - There is no undo for this operation.
        """
        self._client.secrets.kv.v2.delete_metadata_and_all_versions(
            path=secret_name,
            mount_point=mount_point
        )

    def client(self) -> hvac.Client:
        """ Get a reference to the current underlying HVAC client.

            Try to avoid this method except in niche circumstances.
            __getitem__(), create_or_update(), delete(), and patch() should cover almost all use
            cases so resort to this when you need precise, race-free version control or advanced
            policy management, etc.

            Notes
            -----
            This is a method rather than an attribute because Vault reserves the right to replace
            its underlying client should there be errors or disconnections.
        """
        return self._client

    def install(self,
                source_path: str,
                dest_path: Optional[str] = None):
        """ Download a file from Vault and copy it to a file.

            The files are stored in Vault according to a convention,
            since Vault is not a file system.

            Vault is not intended for large files at all! Only use this for small configuration
            files like SSH keys, SSL Certificates, and the like.

            Accepts
            -------
            * source_path: the virtual location of the file in Vault.
            * dest_path: where to save the file on this machine. Defaults to source_path.
                         This implies that by default it will be dropped into the $PWD.
        """
        with open(dest_path or source_path, "w") as dest:
            dest.write(self[source_path.replace('/', '-')])

    @classmethod
    def default(cls, *args, **kwargs):
        """ Create a new Vault, or use an existing one if one has been created.

            Accepts
            -------
            All args and kwargs are passed to Vault.__init__ so see __init__
            for further details.

            Returns
            -------
            Vault
                The singleton Vault for this process

            Notes
            -----
            If you are sure a Vault has already been created, feel free to pass nothing,
            as the arguments will be discarded anyway.
        """
        if cls.SINGLETON is None:
            cls.SINGLETON = Vault(*args, **kwargs)
        return cls.SINGLETON


class VaultAdmin(Vault):
    def __init__(self,
                 namespace=os.environ.get("VAULT_NAMESPACE"),
                 role_id=os.environ.get("VAULT_ROLE_ID"),
                 secret_id=os.environ.get("VAULT_SECRET_ID"),
                 username=None,
                 password=None
                 ):
        """ Log into a specific vault as an administrator using a role_id and secret_id.

            Accepts
            -------
            - `namespace`: str: The namespace of your vault, like "department/auth/team"
                                If unspecified, it will default to $VAULT_NAMESPACE.
                                If neither are specified, it will raise an error.
            - `role_id`: str:   Your app role's role_id.
                                It's like a username but it's a random generated hex code.
                                If unspecified, it will default to $VAULT_ROLE_ID.
                                If neither are specified, it will raise an error.
            - `secret_id`: str: Your app role's secret_id.
                                It's like a password but it's a random generated hex code.
                                If unspecified, it will default to $VAULT_SECRET_ID,
                                and if that is also unspecified it will search your keyring.
                                If none are specified, it will raise an error.

            Notes
            -----
            A role id and secret_id are not much different from a username and password.
            The role_id is okay to store in code, like a username would be,
            but the secret_id can't, because it's like a password.

            Vault doesn't help you store secret_id -  this is a case of catch 22.
            To break the cycle, you need something to store the secret_id.
            This class autodetects several methods of passing that secret.

            - Jenkins will store it for you when building, but if you save it in a Docker image,
              you have to treat the image as secret, which is inconvenient.
              If we find a VAULT_SECRET_ID environment variable, we will use it but not store it.
            - Kubernetes will store secrets for you as well, a good choice for services
            - Keyring will store secrets on interactive machines,
              like Mac, Windows, or GNOME or KDE on Linux. This doesn't include servers.
        """

        # Initialize parent class
        super().__init__(
            namespace=namespace,
            role_id=role_id,
            secret_id=secret_id,
            username=username,
            password=password
        )
        # set namespace
        self.namespace = namespace

    def list_namespaces(self):
        """ Lists all the namespaces within the current namespace.

            Accepts
            -------
            - None

            Returns
            -------
            - List containing all the namespace paths.

            Notes
            -------
            This is a wrapper for hvac.client.sys.list_namespaces method.
        """
        try:
            return self._client.sys.list_namespaces()['data']['keys']
        except hvac.exceptions.InvalidPath:
            return []

    def create_namepsace(self, path: str):
        """ Creates a new namespace within the current namespace.

            Accepts
            -------
            - `path`: str: The path to the new namespace to be generated.
                           Example: 'new_namespace'

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.sys.create_namespace method.
        """
        try:
            self._client.sys.create_namespace(path=path)
        except hvac.exceptions.InvalidRequest:
            print(f"Namespace {path} already exists in {self.namespace}.")

    def delete_namespace(self, path: str):
        """ Deletes a namespace within the current namespace.

            Accepts
            -------
            - `path`: str: The path to the namespace to be removed.
                           Example: 'old_namespace'

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.sys.delete_namespace method.
        """
        self._client.sys.delete_namespace(path=path)

    def list_policies(self):
        """ Lists all the policies within the current namespace.

            Accepts
            -------
            - None

            Returns
            -------
            - List containing all the policy names.

            Notes
            -------
            This is a wrapper for hvac.client.sys.list_policies method.
        """
        return self._client.sys.list_policies()['data']['policies']

    def read_policy(self, name: str):
        """ Reads a given policy.

            Accepts
            -------
            - `name`: str: The name of the policy to be read.
                           Example: 'my_policy'

            Returns
            -------
            - Text of the named policy.

            Notes
            -------
            This is a wrapper for hvac.client.sys.read_policy method.
        """
        return self._client.sys.read_policy(name=name)['data']['rules']

    def create_update_policy(self, name: str, policy: str):
        """ Reads a given policy.

            Accepts
            -------
            - `name`: str:   The name of the policy to be created or updated.
                             Example: 'my_policy'
            - `policy`: str: The text of the policy to be implement.
                             Example:
                             path "sys/*" {
                             capabilities = ["create", "read", "update", "delete", "list", "sudo"]
                             }
                             path "identity/*" {
                             capabilities = ["create", "read", "update", "delete", "list", "sudo"]
                             }
                             path "auth/*" {
                             capabilities = ["create", "read", "update", "delete", "list", "sudo"]
                             }
                             path "secrets/*" {
                             capabilities = ["create", "read", "update", "delete", "list", "sudo"]
                             }
                             path "secret/*" {
                             capabilities = ["create", "read", "update", "delete", "list", "sudo"]
                             }

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.sys.create_or_update method.
        """
        self._client.sys.create_or_update_policy(
            name=name, policy=policy
        )

    def delete_policy(self, name: str):
        """ Deletes a given policy.

            Accepts
            -------
            - `name`: str: The name of the policy to be deleted.
                           Example: 'my_policy'

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.sys.delete_policy method.
        """
        self._client.sys.delete_policy(name=name)

    def enable_secrets_engine(
            self,
            path: str,
            backend_type: str = 'kv',
            description: str = None,
            **kwargs):
        """ Creates a new secrets engine in the current namespace.

            Accepts
            -------
            - `path`: str:         The path for the new secrests engine.
                                   Example: 'my_secret'
            - `backend_type`: str: The type of engine to create, defaults to 'kv' (key-value).
                                   Example: 'kv'
            - `description`: str:  A description of the new secret engine, defaults to None.
                                   Example: 'My super duper top secrets'

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.sys.enable_secrets_engine method.
            Additional key word arguments can be found in the hvac documentation.
            When implement a key-value engine this method will only generate version 2 kv engines.
        """

        # If implementing key-value engine, enforce version 2.
        if backend_type == 'kv':
            if 'options' not in kwargs.keys():
                kwargs['options'] = {'version': 2}
            else:
                kwargs['options']['version'] = 2

        try:
            self._client.sys.enable_secrets_engine(
                backend_type=backend_type, path=path, description=description, **kwargs
            )
        except hvac.exceptions.InvalidRequest:
            print(f'Secret engine {path} already exists in {self.namespace}.')

    def disable_secrets_engine(self, path: str):
        """ Deletes a given secrets engine.

            Accepts
            -------
            - `path`: str: The path of the secrets engine to be deleted.
                           Example: 'my_secret'

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.sys.disable_secrets_engine method.
        """
        self._client.sys.disable_secrets_engine(path)

    def list_secrets_engines(self):
        """ Lists the secrets engines in the current namespace.

            Accepts
            -------
            - None

            Returns
            -------
            - List containing the paths to all the secrets engines in the namespace.

            Notes
            -------
            This is a wrapper for hvac.client.sys.list_mounted_secrets_engines method.
        """
        return list(self._client.sys.list_mounted_secrets_engines()['data'].keys())

    def list_auth_methods(self):
        """ Lists the auth methods in the current namespace.

            Accepts
            -------
            - None

            Returns
            -------
            - List containing the paths to all the auth methods in the namespace.

            Notes
            -------
            This is a wrapper for hvac.client.sys.list_auth_methods method.
        """
        return list(self._client.sys.list_auth_methods()['data'].keys())

    def disable_auth_method(self, path: str):
        """ Deletes a given auth method.

            Accepts
            -------
            - `path`: str: The path of the auth method to be deleted.
                           Example: 'my_auth'

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.sys.disable_auth_method method.
        """
        self._client.sys.disable_auth_method(
            path=path
        )

    def enable_auth_method(self, method: str, path: str, config: dict = None, **kwargs):
        """ Creates a new auth method in the current namespace.

            Accepts
            -------
            - `method`: str:  The type of auth method to create.
                              Example: 'userpass', 'approle'
            - `path`: str:    The path for the new auth method.
                              Example: 'my_auth'
            - `config`: dict: A dictionary of configuration settings, defaults to None.
                              Example: {'default_lease_ttl': '1h', 'max_lease_ttl': '4h'}

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.sys.enable_auth_method method.
            Additional key word arguments can be found in the hvac documentation.
        """
        if method not in ['approle', 'userpass']:
            print(
                f'{method} is not a currently supported Auth Method.'
                + ' Please select either "approle" or "userpass".')
            return

        try:
            self._client.sys.enable_auth_method(
                method_type=method,
                path=path,
                config=config,
                **kwargs
            )
        except hvac.exceptions.InvalidRequest:
            print(
                f'Auth Method {path} already exists in namespace {self.namespace}.')

    def read_auth_method_tuning(self, path):
        """ Reads a given auth method.

            Accepts
            -------
            - `path`: str: The path for of auth method to read.
                           Example: 'my_auth'

            Returns
            -------
            - Text of the named auth method.

            Notes
            -------
            This is a wrapper for hvac.client.sys.read_auth_method_tuning method.
        """
        return self._client.sys.read_auth_method_tuning(path)['data']

    def tune_auth_method(
        self, path: str, default_lease_ttl: str = None,
        max_lease_ttl: str = None, description: str = None
    ):
        """ Updates an existing auth method in the current namespace.

            Accepts
            -------
            - `path`: str:              The path for the auth method to update, defaults to None.
                                        Example: 'my_auth'
            - `default_lease_ttl`: str: The default time limit of the method, defaults to None.
                                        Example: 'my_auth'
            - `max_lease_ttl`: str:     The maximum time limit of the method, defaults to None.
                                        Example: 'my_auth'
            - `description`: str:       The description of the method, defaults to None.
                                        Example: 'my_auth'

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.sys.tune_auth_method method.
        """
        try:
            self._client.sys.tune_auth_method(
                path, default_lease_ttl=default_lease_ttl,
                max_lease_ttl=max_lease_ttl, description=description
            )
        except hvac.exceptions.InvalidRequest:
            print(f'No Auth Method {path} in namespace {self.namespace}.')

    def create_or_update_approle(
        self, role_name: str, mount_point: str = 'approle', token_ttl: str = '1h',
        token_max_ttl: str = '4h', token_policies: list = None, **kwargs
    ):
        """ Creates a new approle or updates an existing one in the current namespace.

            Accepts
            -------
            - `role_name`: str:       The name of the approle to be created or updated.
                                      Example: 'my_approle'
            - `mount_point`: str:     The auth method, under which to place a new approle, or the
                                      auth method under which the approle to be updated resides.
                                      Defaults to 'approle'.
                                      Example: 'my_auth'
            - `token_ttl`: str:       The amount of time before the approle's token needs to be
                                      renewed. Defaults to '1h' (1 hour).
                                      Example: '1h'
            - `token_max_ttl`: str:   The maximum amount of time that the approle's token is good
                                      for, including renewals. Defaults to '4h' (4 hours).
                                      Example: '4h'
            - `token_policies`: list: A list containing the policies to apply to the approle.
                                      Defaults to None.
                                      Example: ['my_policy', 'approle_policy']

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.auth.approle.create_or_update_approle method.
            Additional key word arguments can be found in the hvac documentation.
        """
        self._client.auth.approle.create_or_update_approle(
            role_name=role_name, token_ttl=token_ttl, token_max_ttl=token_max_ttl,
            token_policies=token_policies, mount_point=mount_point, **kwargs
        )

    def delete_role(self, role_name, mount_point='approle'):
        """ Deletes a given approle under the given mount point.

            Accepts
            -------
            - `role_name`: str:   The name of the approle to be deleted.
                                  Example: 'my_approle'
            - `mount_point`: str: The auth method, under which to place a the approle resides.
                                  Defaults to 'approle'.
                                  Example: 'my_auth'

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.auth.approle.delete_role
        """
        self._client.auth.approle.delete_role(
            role_name=role_name, mount_point=mount_point)

    def list_roles(self, mount_point='approle'):
        """ Lists all the approles under the given mount point.

            Accepts
            -------
            - `mount_point`: str: The auth method, under which to look for approles.
                                  Defaults to 'approle'.
                                  Example: 'my_auth'

            Returns
            -------
            - List containing all the approles under the given mount point.

            Notes
            -------
            This is a wrapper for hvac.client.auth.approle.list_roles
        """
        try:
            return self._client.auth.approle.list_roles(mount_point=mount_point)['data']['keys']
        except hvac.exceptions.InvalidPath:
            return []

    def read_role(self, role_name, mount_point='approle'):
        """ Reads a given role at the given mount point.

            Accepts
            -------
            - `role_name`: str:   The name of the approle to be read.
                                  Example: 'my_approle'
            - `mount_point`: str: The auth method, under which the approle to be read resides.
                                  Defaults to 'approle'.
                                  Example: 'my_auth'

            Returns
            -------
            - Text of the named approle.

            Notes
            -------
            This is a wrapper for hvac.client.auth.approle.read_role method.
        """
        try:
            return (
                self
                ._client
                .auth
                .approle
                .read_role(role_name=role_name, mount_point=mount_point)
                ['data']
            )
        except hvac.exceptions.InvalidPath:
            print(
                f'{mount_point}/{role_name} not found in namespace {self.namespace}')

    def read_role_id(self, role_name, mount_point='approle'):
        """ Reads the role id of the given role at the given mount point.

            Accepts
            -------
            - `role_name`: str:   The name of the approle whose rold id is to be read.
                                  Example: 'my_approle'
            - `mount_point`: str: The auth method, under which the approle to be read resides.
                                  Defaults to 'approle'.
                                  Example: 'my_auth'

            Returns
            -------
            - Text of the named approle's role id.

            Notes
            -------
            This is a wrapper for hvac.client.auth.approle.read_role_id method.
        """
        try:
            return (
                self
                ._client
                .auth
                .approle
                .read_role_id(role_name=role_name, mount_point=mount_point)
                ['data']
            )
        except hvac.exceptions.InvalidPath:
            print(
                f'{mount_point}/{role_name} not found in namespace {self.namespace}')

    def generate_secret_id(self, role_name, mount_point='approle'):
        """ Generates a new secret id for the given role at the given mount point.

            Accepts
            -------
            - `role_name`: str:   The name of the approle whose secret id is to be generated.
                                  Example: 'my_approle'
            - `mount_point`: str: The auth method, under which the approle resides.
                                  Defaults to 'approle'.
                                  Example: 'my_auth'

            Returns
            -------
            - Text of the named approle's new secret id.

            Notes
            -------
            This is a wrapper for hvac.client.auth.approle.generate_secret_id method.
        """
        try:
            return (
                self
                ._client
                .auth
                .approle
                .generate_secret_id(role_name=role_name, mount_point=mount_point)
                ['data']
            )
        except hvac.exceptions.InvalidPath:
            print(
                f'{mount_point}/{role_name} not found in namespace {self.namespace}')

    def create_or_update_user(self, username, password, mount_point='userpass'):
        """ Creates a new user or updates an existing one in the current namespace.

            Accepts
            -------
            - `username`: str:        The name of the user to be created or updated.
                                      Example: 'my_user'
            - `password`: str:        The password to assign to a new user, or the new password to
                                      assign for an existing user.
                                      Example: 'password123'
            - `mount_point`: str:     The auth method, under which to place a new approle, or the
                                      auth method under which the approle to be updated resides.
                                      Defaults to 'userpass'.
                                      Example: 'my_auth'

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.auth.userpass.create_or_update_user method.
        """
        self._client.auth.userpass.create_or_update_user(
            username=username, password=password, mount_point=mount_point
        )

    def delete_user(self, username, mount_point='userpass'):
        """ Deletes a given user under the given mount point.

            Accepts
            -------
            - `role_name`: str:       The name of the approle to be deleted.
                                      Example: 'my_approle'
            - `mount_point`: str:     The auth method, under which to place a the approle resides.
                                      Defaults to 'approle'.
                                      Example: 'my_auth'

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.auth.approle.delete_role
        """
        self._client.auth.userpass.delete_user(
            username=username, mount_point=mount_point)

    def list_users(self, mount_point='userpass'):
        """ Lists all the users under the given mount point.

            Accepts
            -------
            - `mount_point`: str:     The auth method, under which to look for users.
                                      Defaults to 'userpass'.
                                      Example: 'my_auth'

            Returns
            -------
            - List containing all the users under the given mount point.

            Notes
            -------
            This is a wrapper for hvac.client.auth.userpass.list_user
        """
        try:
            return self._client.auth.userpass.list_user(mount_point=mount_point)['data']['keys']
        except hvac.exceptions.InvalidPath:
            return []

    def read_user(self, username, mount_point='userpass'):
        """ Reads a given user at the given mount point.

            Accepts
            -------
            - `username`: str:   The name of the user to be read.
                                  Example: 'my_user'
            - `mount_point`: str: The auth method, under which the user to be read resides.
                                  Defaults to 'userpass'.
                                  Example: 'my_auth'

            Returns
            -------
            - Text of the named user.

            Notes
            -------
            This is a wrapper for hvac.client.auth.userpass.read_user method.
        """
        try:
            return (
                self
                ._client
                .auth
                .userpass
                .read_user(username=username, mount_point=mount_point)
                ['data']
            )
        except hvac.exceptions.InvalidPath:
            print(f'{mount_point}/{username} not found in namespace {self.namespace}')

    def change_namespace(self, path: str):
        """ Changes the namespace you are currently logged into.

            Accepts
            -------
            - `path`: str: The namespace you want to login to, like "department/auth/team".

            Returns
            -------
            - None
        """
        self._client.adapter.namespace = path
        try:
            self.list_policies()
        except hvac.exceptions.Forbidden:
            self._client.adapter.namespace = self.namespace
            print('You do not have permission to access this namespace. '
                  + 'Please contact your administrator to update your policy.'
                  )

    def list_secret_id_accessors(self, role_name, mount_point='approle'):
        """ Lists the secret id accessors of the given role at the given mount point.

            Accepts
            -------
            - `role_name`: str:   The name of the approle whose secret id accessors to
                                  are to be read.
                                  Example: 'my_approle'
            - `mount_point`: str: The auth method, under which the approle.
                                  Defaults to 'approle'.
                                  Example: 'my_auth'

            Returns
            -------
            - Text of the named approle's role id.

            Notes
            -------
            This is a wrapper for hvac.client.auth.approle.list_secret_id_accessors method.
        """
        try:
            return (
                self
                ._client
                .auth
                .approle
                .list_secret_id_accessors(role_name=role_name, mount_point=mount_point)
                ['data']['keys']
            )
        except hvac.exceptions.InvalidPath:
            print(
                f'No secret ids found at {mount_point}/{role_name} in namespace {self.namespace}')
        except hvac.exceptions.InvalidRequest:
            print(
                f'No secret ids found at {mount_point}/{role_name} in namespace {self.namespace}')

    def destroy_secret_id_accessor(self, role_name, secret_id_accessor, mount_point='approle'):
        """ Destroys a secret id using the secret id accsessor of the given role at
            the given mount point.

            Accepts
            -------
            - `role_name`: str:          The name of the approle whose secret id is to
                                         be destroyed.
                                         Example: 'my_approle'
            - `secret_id_accessor`: str: The secret id accessor for the secret id that
                                         is to be destroyed
            - `mount_point`: str:        The auth method, under which the approle.
                                         Defaults to 'approle'.
                                         Example: 'my_auth'

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.auth.approle.destroy_secret_id_accessor method.
        """
        try:
            self._client.auth.approle.destroy_secret_id_accessor(
                role_name=role_name, secret_id_accessor=secret_id_accessor,
                mount_point=mount_point
            )
        except hvac.exceptions.InternalServerError:
            print(
                f'{secret_id_accessor} not found'
                f' on {mount_point}/{role_name} in namespace {self.namespace}')
        except hvac.exceptions.InvalidPath:
            print(
                f'{secret_id_accessor} not found'
                f' on {mount_point}/{role_name} in namespace {self.namespace}')

    def destroy_secret_id(self, role_name, secret_id, mount_point='approle'):
        """ Destroys a secret id using the secret id of the given role at
            the given mount point.

            Accepts
            -------
            - `role_name`: str:   The name of the approle whose secret id is to
                                  be destroyed.
                                  Example: 'my_approle'
            - `secret_id`: str:   The secret id accessor for the secret id that
                                  is to be destroyed
            - `mount_point`: str: The auth method, under which the approle.
                                  Defaults to 'approle'.
                                  Example: 'my_auth'

            Returns
            -------
            - None

            Notes
            -------
            This is a wrapper for hvac.client.auth.approle.destroy_secret_id method.
        """
        try:
            self._client.auth.approle.destroy_secret_id(
                role_name=role_name, secret_id=secret_id,
                mount_point=mount_point
            )
        except hvac.exceptions.InternalServerError:
            print(
                f'{secret_id} not found on {mount_point}/{role_name} in namespace {self.namespace}')
        except hvac.exceptions.InvalidPath:
            print(
                f'{secret_id} not found on {mount_point}/{role_name} in namespace {self.namespace}')
