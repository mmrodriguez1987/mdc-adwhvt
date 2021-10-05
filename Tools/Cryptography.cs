using System;
using System.Collections;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace Tools
{
    /// <summary>
    /// En criptografía, RSA (Rivest, Shamir y Adleman) es un sistema criptografico de clave publica desarrollado en 1977. 
    /// Es el primer y mas utilizado algoritmo de este tipo y es valido tanto para cifrar como para firmar digitalmente.
    /// La seguridad de este algoritmo radica en el problema de la factorizacion de numeros enteros. 
    /// Los mensajes enviados se representan mediante numeros, y el funcionamiento se basa en el producto, conocido, 
    /// de dos numeros primos grandes elegidos al azar y mantenidos en secreto. Actualmente estos primos son del orden 
    /// de 10^{200}, y se prevé que su tamaño crezca con el aumento de la capacidad de calculo de los ordenadores.
    /// 
    /// Como en todo sistema de clave publica, cada usuario posee dos claves de cifrado: una publica y otra privada. 
    /// Cuando se quiere enviar un mensaje, el emisor busca la clave publica del receptor, cifra su mensaje con esa 
    /// clave, y una vez que el mensaje cifrado llega al receptor, este se ocupa de descifrarlo usando su clave privada.
    /// 
    /// Se cree que RSA sera seguro mientras no se conozcan formas rapidas de descomponer un numero grande en producto de primos.
    /// La computacion cuantica podria proveer de una solucion a este problema de factorizacion.
    /// </summary>    
    public class RSA
    {

        //private static string patron_busqueda = "0ABIZ2ÑebDNOEcwGl6oSñixq1...";       
        //private static string Patron_encripta = "vQÑO8dk1VgIPZxAR3UsLD6XBz...";

        //private static string m_clave;
        private static UTF8Encoding ue = new UTF8Encoding();
        private static RSACryptoServiceProvider sec = new RSACryptoServiceProvider();

        // private byte[] bytString , bytEncriptar, bytDesEncriptar;


        /// <summary>
        /// Encripta un cadena de mediante encriptacion RSA
        /// </summary>
        /// <param name="EncriptString">Cadena a encriptar</param>
        /// <returns>Cadena Encriptada, si hay algun error devuelve un cero</returns>
        public static String Encriptar(string EncriptString)
        {
            string strEncriptar = string.Empty;
            if (strEncriptar != string.Empty)
            {
                try
                {
                    byte[] bytString = ue.GetBytes(EncriptString);
                    byte[] bytEncriptar = sec.Encrypt(bytString, false);
                    strEncriptar = Convert.ToBase64String(bytEncriptar);
                }
                catch (Exception er)
                {
                    throw new Exception("Error al cifrar la informacion", er);
                }
                return strEncriptar;
            }
            else
            {
                throw new Exception("La variable EncriptString esta vacia");
            }
        }



        /// <summary>
        /// Encripta un arreglo unidimensional de mediante encriptacion RSA
        /// </summary>
        /// <param name="EncriptString">Cadena a encriptar</param>
        /// <returns>Cadena Encriptada, si hay algun error disparar una Excepcion</returns>
        public static String[] Encriptar(string[] EncriptString)
        {
            for (int i = 0; i < EncriptString.Length; i++)
            {
                if (EncriptString[i] != string.Empty)
                {
                    try
                    {
                        EncriptString[i] = Encriptar(EncriptString[i]);
                    }
                    catch (Exception er)
                    {
                        throw new Exception("Error al cifrar el arreglo", er);
                    }
                }

            }
            return EncriptString;
        }

        /// <summary>
        /// Decripta un cadena con codificacion RSA
        /// </summary>
        /// <param name="EncriptString">Cadena a decriptar</param>
        /// <returns>Cadena Encriptada, si hay algun error devuelve un cero</returns>   
        public static String Decriptar(string EncriptString)
        {
            string strDesencriptar = string.Empty;
            if (EncriptString != string.Empty)
            {
                try
                {
                    byte[] bytDesEncriptar = sec.Decrypt(Convert.FromBase64String(EncriptString), false);
                    strDesencriptar = ue.GetString(bytDesEncriptar);
                }
                catch (Exception er)
                {
                    throw new Exception("Error al decifrar la informacion", er);
                }
                return strDesencriptar;
            }
            else
            {
                throw new Exception("La variable EncriptString esta vacia");
            }
        }


        /// <summary>
        /// Decripta un arreglo unidimensional de con cifrado RSA
        /// </summary>
        /// <param name="EncriptString">Arreglo de parametros encriptado</param>
        /// <returns>Cadena decriptada, si hay algun error dispara una Excepcion</returns>
        public static String[] Decriptar(string[] EncriptString)
        {
            for (int i = 0; i < EncriptString.Length; i++)
            {
                if (EncriptString[i] != string.Empty)
                {
                    try
                    {
                        EncriptString[i] = Decriptar(EncriptString[i]);
                    }
                    catch (Exception er)
                    {
                        throw new Exception("Error al cifrar el arreglo", er);
                    }
                }

            }
            return EncriptString;
        }


        /// <summary>
        /// Encripta una cadena de texto con un tamaño especifico de la llave y la llave misma
        /// </summary>
        /// <param name="inputString">Cadena que se desea cifrar</param>
        /// <param name="dwKeySize">Tamaño de la llave</param>
        /// <param name="xmlString">Clave sobre la cual se va a cifrar</param>
        /// <returns>Retorna la cadena encriptada</returns>
        public static string Encriptar(string inputString, int dwKeySize, string xmlString)
        {
            // TODO: Add Proper Exception Handlers
            RSACryptoServiceProvider rsaCryptoServiceProvider = new RSACryptoServiceProvider(dwKeySize);
            rsaCryptoServiceProvider.FromXmlString(xmlString);
            int keySize = dwKeySize / 8;
            byte[] bytes = Encoding.UTF32.GetBytes(inputString);

            int maxLength = keySize - 42;
            int dataLength = bytes.Length;
            int iterations = dataLength / maxLength;
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i <= iterations; i++)
            {
                byte[] tempBytes = new byte[(dataLength - maxLength * i > maxLength) ? maxLength : dataLength - maxLength * i];
                Buffer.BlockCopy(bytes, maxLength * i, tempBytes, 0, tempBytes.Length);
                byte[] encryptedBytes = rsaCryptoServiceProvider.Encrypt(tempBytes, true);
                Array.Reverse(encryptedBytes);
                stringBuilder.Append(Convert.ToBase64String(encryptedBytes));
            }
            return stringBuilder.ToString();
        }


        /// <summary>
        /// Decripta una cadena de texto cifrada con RSA con un tamaño especifico de la llave y la llave misma
        /// </summary>
        /// <param name="inputString">Cadena que se desea cifrar</param>
        /// <param name="dwKeySize">Tamaño de la llave</param>
        /// <param name="xmlString">Clave sobre la cual se va a cifrar</param>
        /// <returns>Retorna la cadena decriptada</returns>
        public string Decriptar(string inputString, int dwKeySize, string xmlString)
        {
            // TODO: Add Proper Exception Handlers
            RSACryptoServiceProvider rsaCryptoServiceProvider = new RSACryptoServiceProvider(dwKeySize);
            rsaCryptoServiceProvider.FromXmlString(xmlString);
            int base64BlockSize = ((dwKeySize / 8) % 3 != 0) ? (((dwKeySize / 8) / 3) * 4) + 4 : ((dwKeySize / 8) / 3) * 4;
            int iterations = inputString.Length / base64BlockSize;
            ArrayList arrayList = new ArrayList();
            for (int i = 0; i < iterations; i++)
            {
                byte[] encryptedBytes = Convert.FromBase64String(inputString.Substring(base64BlockSize * i, base64BlockSize));
                Array.Reverse(encryptedBytes);
                arrayList.AddRange(rsaCryptoServiceProvider.Decrypt(encryptedBytes, true));
            }
            return Encoding.UTF32.GetString(arrayList.ToArray(Type.GetType("System.Byte")) as byte[]);
        }
    }





   
}
