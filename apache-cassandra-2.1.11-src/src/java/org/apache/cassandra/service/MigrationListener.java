
public abstract class MigrationListener
{
    public void onCreateKeyspace(String ksName) {}
    public void onCreateColumnFamily(String ksName, String cfName) {}
    public void onCreateUserType(String ksName, String typeName) {}

    public void onUpdateKeyspace(String ksName) {}
    public void onUpdateColumnFamily(String ksName, String cfName, boolean columnsDidChange) {}
    public void onUpdateUserType(String ksName, String typeName) {}

    public void onDropKeyspace(String ksName) {}
    public void onDropColumnFamily(String ksName, String cfName) {}
    public void onDropUserType(String ksName, String typeName) {}
}
