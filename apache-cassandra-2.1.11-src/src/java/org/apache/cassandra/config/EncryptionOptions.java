

public abstract class EncryptionOptions
{
    public String keystore = "conf/.keystore";
    public String keystore_password = "cassandra";
    public String truststore = "conf/.truststore";
    public String truststore_password = "cassandra";
    public String[] cipher_suites = {
        "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA" 
    };
    public String protocol = "TLS";
    public String algorithm = "SunX509";
    public String store_type = "JKS";
    public boolean require_client_auth = false;

    public static class ClientEncryptionOptions extends EncryptionOptions
    {
        public boolean enabled = false;
    }

    public static class ServerEncryptionOptions extends EncryptionOptions
    {
        public static enum InternodeEncryption
        {
            all, none, dc, rack
        }
        public InternodeEncryption internode_encryption = InternodeEncryption.none;
    }
}
