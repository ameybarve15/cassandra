

public interface ISaslAwareAuthenticator extends IAuthenticator
{
    /**
     * Provide a SaslAuthenticator to be used by the CQL binary protocol server. If
     * the configured IAuthenticator requires authentication but does not implement this
     * interface we refuse to start the binary protocol server as it will have no way
     * of authenticating clients.
     * @return SaslAuthenticator implementation
     * (see {@link PasswordAuthenticator.PlainTextSaslAuthenticator})
     */
    SaslAuthenticator newAuthenticator();


    public interface SaslAuthenticator
    {
        public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException;
        public boolean isComplete();
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException;
    }
}
