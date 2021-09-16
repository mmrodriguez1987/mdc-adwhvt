using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Mail;
using System.Net.Mime;
using System.Text;

namespace Tools
{



    public class Mail
    {
        #region Variables Miembros
        private string p_Para, p_File;
        private string p_Asunto;
        private string p_Mensaje;
        private string p_DisplayName;
        private string p_De;
        private string p_CC;
        private string p_Contraseña;
        private string p_ServerMail;
        private Boolean p_IsBodyHtml = true;
        private MailPriority p_Prioridad = MailPriority.Normal;
        private int p_Puerto = 25;
        private Boolean p_EnableSsl = false;
        #endregion

        #region Atributos
        /// <summary>
        /// Cadena de Email separados por comas a los que se
        /// le enviara copia del Email original
        /// </summary>
        public string ConCopia
        {
            get
            {
                return p_CC;
            }
            set
            {
                p_CC = value;
            }
        }
        /// <summary>
        /// Nombre que se mostrara en el "DE" del Email
        /// </summary>
        public string DisplayName
        {
            get
            {
                return p_DisplayName;
            }
            set
            {
                p_DisplayName = value;
            }
        }
        /// <summary>
        /// Especifica si se usara o no autenticacion SSL
        /// </summary>
        public Boolean EnableSsl
        {
            get
            {
                return p_EnableSsl;
            }
            set
            {
                p_EnableSsl = value;
            }
        }
        /// <summary>
        /// Contraseña de la cuenta que envia el correo
        /// </summary>
        public string Contraseña
        {
            get
            {
                return p_Contraseña;
            }
            set
            {
                p_Contraseña = value;
            }
        }
        /// <summary>
        /// Correo electronico de donde se envia el Mensaje
        /// </summary>
        public string De
        {
            get
            {
                return p_De;
            }
            set
            {
                p_De = value;
            }
        }
        /// <summary>
        /// Espeficica si el correo que se envia se mostrara en Formato HTML
        /// </summary>
        public Boolean IsBodyHtml
        {
            get
            {
                return p_IsBodyHtml;
            }
            set
            {
                p_IsBodyHtml = value;
            }
        }
        /// <summary>
        /// Cuerpo del Mensaje
        /// </summary>
        public string Mensaje
        {
            get
            {
                return p_Mensaje;
            }
            set
            {
                p_Mensaje = value;
            }
        }
        /// <summary>
        /// Asunto del Mensaje
        /// </summary>
        public string Asunto
        {
            get
            {
                return p_Asunto;
            }
            set
            {
                p_Asunto = value;
            }
        }
        /// <summary>
        /// Cadena de Correos electronicos separados por coma a los cuales se enviara el mensaje
        /// </summary>
        public string Para
        {
            get
            {
                return p_Para;
            }
            set
            {
                p_Para = value;
            }
        }

        /// <summary>
        /// Ruta del Archivo Adjunto
        /// </summary>
        public string File
        {
            get
            {
                return p_File;
            }
            set
            {
                p_File = value;
            }
        }

        /// <summary>
        /// Puerto que utiliza la bandeja de salida
        /// </summary>
        public int Puerto
        {
            get
            {
                return p_Puerto;
            }
            set
            {
                p_Puerto = value;
            }
        }
        /// <summary>
        /// Prioridad del Menesaje
        /// </summary>
        public MailPriority Prioridad
        {
            get
            {
                return p_Prioridad;
            }
            set
            {
                p_Prioridad = value;
            }
        }
        #endregion

        #region Constructor de la Clase
        /// <summary>
        /// Constructor de la clase
        /// </summary>
        /// <param name="ServerMail">Host del Servidor de Correos</param>       
        public Mail(string ServerMail)
        {
            this.p_ServerMail = ServerMail;
        }
        #endregion

        #region Metodos
        /// <summary>
        /// Metodo que envia el correo electronico
        /// </summary>
        /// <exception cref="System.Net.Mail.SmtpException">Error generado en el motor de correos</exception>       
        public void SendMail()
        {
            try
            {
                MailMessage msg = new MailMessage();
                SmtpClient smtp = new SmtpClient();

                smtp.Credentials = new NetworkCredential(p_De, p_Contraseña);
                smtp.Host = p_ServerMail;
                smtp.Port = p_Puerto;
                smtp.EnableSsl = p_EnableSsl;

                msg.To.Add(p_Para);
                msg.From = new MailAddress(p_De, p_DisplayName, System.Text.Encoding.UTF8);
                msg.Subject = p_Asunto;
                msg.SubjectEncoding = System.Text.Encoding.UTF8;
                msg.Body = p_Mensaje;
                msg.BodyEncoding = System.Text.Encoding.UTF8;
                msg.CC.Add(p_CC);
                msg.Priority = p_Prioridad;
                if (p_File != String.Empty)
                {
                    msg.Attachments.Add(new Attachment(p_File, MediaTypeNames.Application.Octet));
                }
                msg.IsBodyHtml = p_IsBodyHtml;
                smtp.Send(msg);
            }
            catch (SmtpException e)
            {
                throw new SmtpException("Error en el envio del correo electronico: ", e);
            }
        }
        #endregion
    }
}
