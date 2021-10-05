using System;


namespace Tools.Documentation
{
    /// <summary>
    /// Class that create author name in assembly as attribute
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface | AttributeTargets.Class | AttributeTargets.Method | AttributeTargets.Constructor, AllowMultiple = true)]
    public class Author : System.Attribute
    {
        private string name, creationDate, description;


        /// <summary>
        /// Assambly Author
        /// </summary>
        /// <param name="name">Architec name</param>
        public Author(string name)
        {
            this.name = name;
            this.creationDate = DateTime.Today.ToShortDateString();
        }
        /// <summary>
        ///Assambly Author
        /// </summary>
        /// <param name="name">Architec name</param>
        /// <param name="date">Creation date</param>
        public Author(string name, string date)
        {
            this.name = name;
            this.creationDate = date;
        }
        /// <summary>
        /// Autor del Ensamblado
        /// </summary>
        /// <param name="name">Architec name</param>
        /// <param name="date">Creation date</param>
        /// <param name="descript">Tecnical description</param>
        public Author(string name, string date, string descript)
        {
            this.name = name;
            this.creationDate = date;
            this.description = descript;
        }
    }

    /// <summary>
    /// class that creates the version of the method in the assembly
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface | AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = true)]
    public class AssemblyVersion : System.Attribute
    {
        private string p_architect;
        private double p_version;
        private string p_date;
        private string p_description;

        /// <summary>
        /// assembly version for method
        /// </summary>
        /// <param name="version">Version number</param>
        /// <param name="architect">Architect name</param>
        public AssemblyVersion(double version, string architect)
        {
            this.p_date = DateTime.Today.ToShortDateString();
            this.p_architect = architect;
            this.p_version = version;
        }

        /// <summary>
        /// assembly version for method
        /// </summary>
        /// <param name="version">Version number</param>
        /// <param name="architect">Architect name</param>
        /// <param name="description">Assambly description</param>
        public AssemblyVersion(double version, string architect, string description)
        {
            this.p_date = DateTime.Today.ToShortDateString();
            this.p_architect = architect;
            this.p_version = version;
            this.p_description = description;
        }

        /// <summary>
        /// assembly version for method 
        /// </summary>
        /// <param name="version">Version number</param>
        /// <param name="architect">Architect name</param>
        /// <param name="description">Assambly description</param>
        /// <param name="date">updated date of the assambly</param>
        public AssemblyVersion(double version, string architect, string description, string date)
        {
            this.p_date = date;
            this.p_architect = architect;
            this.p_version = version;
            this.p_description = description;
        }
    }

    /// <summary>
    /// Class that creates in the assembly the help provided by the author
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface | AttributeTargets.Class | AttributeTargets.Method | AttributeTargets.Constructor, AllowMultiple = true)]
    public class Helper : System.Attribute
    {
        private string Link, topic;

        /// <summary>
        /// Assambly help
        /// </summary>
        /// <param name="lnk">Help link</param>
        public Helper(string lnk)
        {
            this.Link = lnk;
        }
        /// <summary>
        ///  Assambly help
        /// </summary>
        /// <param name="lnk">Help link</param>
        /// <param name="topic">Topic name</param>
        public Helper(string lnk, string topic)
        {
            this.Link = lnk;
            this.topic = topic;
        }
    }


   
    [AttributeUsage(AttributeTargets.Interface | AttributeTargets.Class | AttributeTargets.Method | AttributeTargets.Constructor, AllowMultiple = true)]
    public class Description : System.Attribute
    {
        private string p_sumary, p_remark, p_param1, p_param2, p_param3, p_param4, p_param5, p_param6;


        public Description(String summary, String remark)
        {
            this.p_sumary = summary;
            this.p_remark = remark;
        }

        public Description(String summary)
        {
            this.p_sumary = summary;            
        }

        public Description(String summary, String remark, String param1)
        {
            this.p_sumary = summary;
            this.p_remark = remark;
            this.p_param1 = param1;
        }


        public Description(String summary, String remark, String param1, String param2)
        {
            this.p_sumary = summary;
            this.p_remark = remark;
            this.p_param1 = param1;
            this.p_param2 = param2;
        }


        public Description(String summary, String remark, String param1, String param2, String param3)
        {
            this.p_sumary = summary;
            this.p_remark = remark;
            this.p_param1 = param1;
            this.p_param2 = param2;
            this.p_param3 = param3;
        }

        public Description(String summary, String remark, String param1, String param2, String param3, String param4)
        {
            this.p_sumary = summary;
            this.p_remark = remark;
            this.p_param1 = param1;
            this.p_param2 = param2;
            this.p_param3 = param3;
            this.p_param4 = param4;
        }

        public Description(String summary, String remark, String param1, String param2, String param3, String param4, String param5)
        {
            this.p_sumary = summary;
            this.p_remark = remark;
            this.p_param1 = param1;
            this.p_param2 = param2;
            this.p_param3 = param3;
            this.p_param4 = param4;
            this.p_param5 = param5;
        }

        public Description(String summary, String remark, String param1, String param2, String param3, String param4, String param5, String param6)
        {
            this.p_sumary = summary;
            this.p_remark = remark;
            this.p_param1 = param1;
            this.p_param2 = param2;
            this.p_param3 = param3;
            this.p_param4 = param4;
            this.p_param5 = param5;
            this.p_param6 = param6;

        }

    }
}
