

public class Locks
{
    static final Unsafe unsafe;

    static
    {
        Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        field.setAccessible(true);
        unsafe = (sun.misc.Unsafe) field.get(null);
    }

    // enters the object's monitor IF UNSAFE IS PRESENT. If it isn't, this is a no-op.
    public static void monitorEnterUnsafe(Object object)
    {
        unsafe.monitorEnter(object);
    }

    public static void monitorExitUnsafe(Object object)
    {
        unsafe.monitorExit(object);
    }
}
