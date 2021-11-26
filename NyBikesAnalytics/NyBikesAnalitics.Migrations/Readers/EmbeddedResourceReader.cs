namespace NyBikesAnalitics.Migrations.Readers
{
    using System.IO;
    using System.Reflection;

    public class EmbeddedResourceReader
    {
        private readonly Assembly assembly;

        public EmbeddedResourceReader(Assembly assembly)
        {
            this.assembly = assembly;
        }

        public string GetEmbeddedResource(string resourceName)
        {
            resourceName = FormatResourceName(this.assembly, resourceName);
            using (Stream resourceStream = this.assembly.GetManifestResourceStream(resourceName))
            {
                if (resourceStream == null)
                {
                    return null;
                }

                using (StreamReader reader = new StreamReader(resourceStream))
                {
                    return reader.ReadToEnd();
                }
            }
        }

        private static string FormatResourceName(Assembly assembly, string resourceName)
        {
            return assembly.GetName().Name + "." + resourceName.Replace(" ", "_")
                       .Replace("\\", ".")
                       .Replace("/", ".");
        }
    }
}
