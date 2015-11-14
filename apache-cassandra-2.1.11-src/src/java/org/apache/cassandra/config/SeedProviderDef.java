

public class SeedProviderDef
{
    public String class_name;
    public Map<String, String> parameters;

    public SeedProviderDef(LinkedHashMap<String, ?> p)
    {
        class_name = (String)p.get("class_name");
        parameters = (Map<String, String>)((List)p.get("parameters")).get(0);
    }
}
