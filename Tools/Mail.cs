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
        #region members
        private string to, file;
        private string subject;
        private string message;
        private string displayName;
        private string from;
        private string withCopy;
        private string password;
        private string serverMail;
        private Boolean isBodyHtml = true;
        private MailPriority priority = MailPriority.Normal;
        private int port;
        private Boolean enableSsl = false;
        #endregion

        #region Attributes
        /// <summary>
        /// Cadena de Email separados por comas a los que se
        /// le enviara copia del Email original
        /// </summary>
        public string CC
        {
            get
            {
                return withCopy;
            }
            set
            {
                withCopy = value;
            }
        }
        /// <summary>
        /// Nombre que se mostrara en el "DE" del Email
        /// </summary>
        public string DisplayName
        {
            get
            {
                return displayName;
            }
            set
            {
                displayName = value;
            }
        }
        /// <summary>
        /// Especifica si se usara o no autenticacion SSL
        /// </summary>
        public Boolean EnableSsl
        {
            get
            {
                return enableSsl;
            }
            set
            {
                enableSsl = value;
            }
        }
        /// <summary>
        /// Contraseña de la cuenta que envia el correo
        /// </summary>
        public string Password
        {
            get
            {
                return password;
            }
            set
            {
                password = value;
            }
        }
        /// <summary>
        /// Correo electronico de donde se envia el Mensaje
        /// </summary>
        public string From
        {
            get
            {
                return from;
            }
            set
            {
                from = value;
            }
        }
        /// <summary>
        /// Espeficica si el correo que se envia se mostrara en Formato HTML
        /// </summary>
        public Boolean IsBodyHtml
        {
            get
            {
                return isBodyHtml;
            }
            set
            {
                isBodyHtml = value;
            }
        }
        /// <summary>
        /// Cuerpo del Mensaje
        /// </summary>
        public string Message
        {
            get
            {
                return message;
            }
            set
            {
                message = value;
            }
        }
        /// <summary>
        /// Asunto del Mensaje
        /// </summary>
        public string Subject
        {
            get
            {
                return subject;
            }
            set
            {
                subject = value;
            }
        }
        /// <summary>
        /// Cadena de Correos electronicos separados por coma a los cuales se enviara el mensaje
        /// </summary>
        public string To
        {
            get
            {
                return to;
            }
            set
            {
                to = value;
            }
        }

        /// <summary>
        /// Ruta del Archivo Adjunto
        /// </summary>
        public string File
        {
            get
            {
                return file;
            }
            set
            {
                file = value;
            }
        }

        /// <summary>
        /// Puerto que utiliza la bandeja de salida
        /// </summary>
        public int Port
        {
            get
            {
                return port;
            }
            set
            {
                port = value;
            }
        }
        /// <summary>
        /// Prioridad del Menesaje
        /// </summary>
        public MailPriority Priority
        {
            get
            {
                return priority;
            }
            set
            {
                priority = value;
            }
        }
        #endregion

        #region Constructor
        /// <summary>
        /// Constructor de la clase
        /// </summary>
        /// <param name="server_mail">Host del Servidor de Correos</param>       
        public Mail(string server_mail)
        {
            serverMail = server_mail;
        }
        #endregion

        #region Methods
        /// <summary>
        /// Metodo que envia el correo electronico
        /// </summary>
        /// <exception cref="System.Net.Mail.SmtpException">Error generado en el motor de correos</exception>       
        public void SendMail()
        {
            try
            {
                MailMessage msg = new MailMessage();
                SmtpClient smtp = new SmtpClient(serverMail);

                smtp.Credentials = new NetworkCredential(from, password);               
                smtp.Port = port;
                
                smtp.EnableSsl = enableSsl;
                
                msg.To.Add(to);
                msg.From = new MailAddress(from, displayName);
                msg.Subject = subject;
                //msg.SubjectEncoding = System.Text.Encoding.UTF8;
                msg.Body = message;
                //msg.BodyEncoding = System.Text.Encoding.UTF8;               
                //msg.Priority = priority;               
                msg.IsBodyHtml = IsBodyHtml;

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
