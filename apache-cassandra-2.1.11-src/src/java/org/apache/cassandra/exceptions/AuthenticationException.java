

public class AuthenticationException extends RequestValidationException
{
    public AuthenticationException(String msg)
    {
        super(ExceptionCode.BAD_CREDENTIALS, msg);
    }
}
