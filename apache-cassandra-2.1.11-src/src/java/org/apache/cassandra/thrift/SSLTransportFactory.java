

public class SSLTransportFactory implements ITransportFactory
{
    public static final String TRUSTSTORE = "enc.truststore";
    public static final String TRUSTSTORE_PASSWORD = "enc.truststore.password";
    public static final String KEYSTORE = "enc.keystore";
    public static final String KEYSTORE_PASSWORD = "enc.keystore.password";
    public static final String PROTOCOL = "enc.protocol";
    public static final String CIPHER_SUITES = "enc.cipher.suites";
    public static final int SOCKET_TIMEOUT = 0;

    private static final Set<String> SUPPORTED_OPTIONS = Sets.newHashSet(TRUSTSTORE,
                                                                         TRUSTSTORE_PASSWORD,
                                                                         KEYSTORE,
                                                                         KEYSTORE_PASSWORD,
                                                                         PROTOCOL,
                                                                         CIPHER_SUITES);

    private String truststore;
    private String truststorePassword;
    private String keystore;
    private String keystorePassword;
    private String protocol;
    private String[] cipherSuites;

    @Override
    public TTransport openTransport(String host, int port)
    {
        TSSLTransportFactory.TSSLTransportParameters params 
            = new TSSLTransportFactory.TSSLTransportParameters(protocol, cipherSuites);
        params.setTrustStore(truststore, truststorePassword);
        params.setKeyStore(keystore, keystorePassword);
        
        TTransport trans = TSSLTransportFactory.getClientSocket(host, port, SOCKET_TIMEOUT, params);
        int frameSize = 15 * 1024 * 1024; // 15 MiB
        
        return new TFramedTransport(trans, frameSize);
    }

    @Override
    public void setOptions(Map<String, String> options)
    {
        truststore = options.get(TRUSTSTORE);
        truststorePassword = options.get(TRUSTSTORE_PASSWORD);
        keystore = options.get(KEYSTORE);
        keystorePassword = options.get(KEYSTORE_PASSWORD);
        protocol = options.get(PROTOCOL);
        cipherSuites = options.get(CIPHER_SUITES).split(",");
    }

    @Override
    public Set<String> supportedOptions()
    {
        return SUPPORTED_OPTIONS;
    }
}
