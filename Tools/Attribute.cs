using System;
using System.Collections.Generic;
using System.Text;

namespace Tools
{
    /// <summary>
    /// Clase que Crea en el ensamblado el nombre del Autor como un atributo
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface | AttributeTargets.Class | AttributeTargets.Method | AttributeTargets.Constructor, AllowMultiple = true)]
    public class Autor : System.Attribute
    {
        private string name, fechacreacion, descripcion;


        /// <summary>
        /// Autor del Ensamblado
        /// </summary>
        /// <param name="name">Nombre completo del Arquitecto</param>
        public Autor(string name)
        {
            this.name = name;
            this.fechacreacion = DateTime.Today.ToShortDateString();
        }
        /// <summary>
        /// Autor del Ensamblado
        /// </summary>
        /// <param name="name">Nombre completo del Arquitecto</param>
        /// <param name="fecha">Fecha de Creación</param>
        public Autor(string name, string fecha)
        {
            this.name = name;
            this.fechacreacion = fecha;
        }
        /// <summary>
        /// Autor del Ensamblado
        /// </summary>
        /// <param name="name">Nombre completo del Arquitecto</param>
        /// <param name="fecha">Fecha de Creación</param>
        /// <param name="descrip">Descripcion Tecnica</param>
        public Autor(string name, string fecha, string descrip)
        {
            this.name = name;
            this.fechacreacion = fecha;
            this.descripcion = descrip;
        }
    }

    /// <summary>
    /// Clase que Crea en el ensamblado la Version del Metodo
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface | AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = true)]
    public class AssemblyVersion : System.Attribute
    {
        private string p_arquitecto;
        private double p_version;
        private string p_fecha;
        private string p_descripcion;

        /// <summary>
        /// Version del Ensamblado para el metodo
        /// </summary>
        /// <param name="version">Numero de la Version</param>
        /// <param name="arquitecto">Nombre del Arquitecto</param>
        public AssemblyVersion(double version, string arquitecto)
        {
            this.p_fecha = DateTime.Today.ToShortDateString();
            this.p_arquitecto = arquitecto;
            this.p_version = version;
        }

        /// <summary>
        /// Version del Ensamblado para el metodo
        /// </summary>
        /// <param name="version">Numero de la Version</param>
        /// <param name="arquitecto">Nombre del Arquitecto</param>
        /// <param name="descripcion">Descripcion del Ensamblado</param>
        public AssemblyVersion(double version, string arquitecto, string descripcion)
        {
            this.p_fecha = DateTime.Today.ToShortDateString();
            this.p_arquitecto = arquitecto;
            this.p_version = version;
            this.p_descripcion = descripcion;
        }

        /// <summary>
        /// Version del Ensamblado para el metodo 
        /// </summary>
        /// <param name="version">Numero de la Version</param>
        /// <param name="arquitecto">Nombre del Arquitecto</param>
        /// <param name="descripcion">Descripcion del Ensamblado</param>
        /// <param name="fecha">Fecha en que se efectuo el cambio</param>
        public AssemblyVersion(double version, string arquitecto, string descripcion, string fecha)
        {
            this.p_fecha = fecha;
            this.p_arquitecto = arquitecto;
            this.p_version = version;
            this.p_descripcion = descripcion;
        }
    }

    /// <summary>
    /// Clase que Crea en el ensamblado la ayuda proveida por el autor
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface | AttributeTargets.Class | AttributeTargets.Method | AttributeTargets.Constructor, AllowMultiple = true)]
    public class Helper : System.Attribute
    {
        private string Link, topic;

        /// <summary>
        /// Ayuda del Ensamblado
        /// </summary>
        /// <param name="lnk">Link de la ayuda</param>
        public Helper(string lnk)
        {
            this.Link = lnk;
        }
        /// <summary>
        /// Ayuda del Ensamblado
        /// </summary>
        /// <param name="lnk">Link de la ayuda</param>
        /// <param name="topic">Topico de la ayuda</param>
        public Helper(string lnk, string topic)
        {
            this.Link = lnk;
            this.topic = topic;
        }
    }
}
