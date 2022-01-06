using System;
using System.Collections.Generic;
using System.Text;

using Azure;
using Azure.Communication;
using Azure.Communication.Sms;

namespace Tools.Communication
{
    public class SMS
    {
        private string ccn;
        private string smsID;
        private SmsClient smsClient;
        private SmsSendResult sendResult;

        public string SmsID { get => smsID; set => smsID = value; }


        public SMS(string com_serv_ccn)
        {
            ccn = com_serv_ccn;
        }

      
        /// <summary>
        /// Send SMS to one recipient
        /// </summary>
        /// <param name="fromPhone">From Phone</param>
        /// <param name="toPhone">To phone Number</param>
        /// <param name="message">Content of the message</param>
        /// <returns>Return a empty string if no error, if there any error return a string with the error</returns>
        public string SendSMS(string fromPhone, string toPhone, string message)
        {
            try
            {
                smsClient = new SmsClient(ccn);
                sendResult = smsClient.Send(
                    from: fromPhone,
                    to: toPhone,
                    message: message
                );
               
                Console.WriteLine(sendResult.MessageId.ToString());
                return "";
            } 
            catch(Exception e)
            {
                return "Error sending the SMS, detail: " + e.ToString();
            }
        }

        /// <summary>
        /// Send a SMS to many recipients represented in one array
        /// </summary>
        /// <param name="fromPhone">From Phone</param>
        /// <param name="toPhone">Recipients phones in string array</param>
        /// <param name="message">Content of the message</param>
        /// <returns>Return a empty string if no error, if there any error return a string with the error</returns>
        public string SendSMS(string fromPhone, string[] toPhone, string message)
        {
            try
            {
                smsClient = new SmsClient(ccn);
                Response<IReadOnlyList<SmsSendResult>> response = smsClient.Send(
                    from: fromPhone,
                    to:  toPhone,
                    message: message
                );

                Console.WriteLine(sendResult.MessageId.ToString());
                return "";
            }
            catch (Exception e)
            {
                return "Error sending the SMS, detail: " + e.ToString();
            }
        }
    }
}
