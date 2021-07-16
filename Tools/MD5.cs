using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace Tools
{
    /// <summary>
    /// MD5 es uno de los algoritmos de reduccion criptograficos diseñados por el profesor Ronald Rivest del MIT 
    /// (Massachusetts Institute of Technology, Instituto Tecnologico de Massachusetts). Fue desarrollado en 1991 
    /// como reemplazo del algoritmo MD4 después de que Hans Dobbertin descubriese su debilidad.
    ///
    /// A pesar de su amplia difusion actual, la sucesion de problemas de seguridad detectados desde que, en 1996, 
    /// Hans Dobbertin anunciase una colision de hash, plantea una serie de dudas acerca de su uso futuro.
    /// </summary>
    public class MD5
    {
        /// <summary>
        /// Clave de encriptacion
        /// </summary>
        private static string key = "MiamiDadeCounty";


        /// <summary>
        /// Encripta un texto utilizando el algoritmo de encriptacion MD5
        /// </summary>
        /// <param name="EncriptString">Cadena a encriptar</param>
        /// <returns></returns>
        public static string Encriptar(string EncriptString)
        {

            //arreglo de bytes donde guardaremos la llave
            byte[] keyArray;
            //arreglo de bytes donde guardaremos el texto
            //que vamos a encriptar
            byte[] Arreglo_a_Cifrar = UTF8Encoding.UTF8.GetBytes(EncriptString);

            //se utilizan las clases de encriptacion
            //provistas por el Framework
            //Algoritmo MD5
            MD5CryptoServiceProvider hashmd5 = new MD5CryptoServiceProvider();
            //se guarda la llave para que se le realice
            //hashing
            keyArray = hashmd5.ComputeHash(UTF8Encoding.UTF8.GetBytes(key));

            hashmd5.Clear();

            //Algoritmo 3DAS
            TripleDESCryptoServiceProvider tdes = new TripleDESCryptoServiceProvider();

            tdes.Key = keyArray;
            tdes.Mode = CipherMode.ECB;
            tdes.Padding = PaddingMode.PKCS7;

            //se empieza con la transformacion de la cadena
            ICryptoTransform cTransform = tdes.CreateEncryptor();

            //arreglo de bytes donde se guarda la
            //cadena cifrada
            byte[] ArrayResultado = cTransform.TransformFinalBlock(Arreglo_a_Cifrar, 0, Arreglo_a_Cifrar.Length);

            tdes.Clear();

            //se regresa el resultado en forma de una cadena
            return Convert.ToBase64String(ArrayResultado, 0, ArrayResultado.Length);
        }

        /// <summary>
        /// Decripta una cadena de texto cifrada con algoritmo MD5
        /// </summary>
        /// <param name="textoEncriptado">Texto encriptado</param>
        /// <returns></returns>
        public static string Desencriptar(string textoEncriptado)
        {
            byte[] keyArray;
            //convierte el texto en una secuencia de bytes
            byte[] Array_a_Descifrar = Convert.FromBase64String(textoEncriptado);

            //se llama a las clases que tienen los algoritmos
            //de encriptacion se le aplica hashing
            //algoritmo MD5
            MD5CryptoServiceProvider hashmd5 = new MD5CryptoServiceProvider();

            keyArray = hashmd5.ComputeHash(UTF8Encoding.UTF8.GetBytes(key));

            hashmd5.Clear();

            TripleDESCryptoServiceProvider tdes = new TripleDESCryptoServiceProvider();

            tdes.Key = keyArray;
            tdes.Mode = CipherMode.ECB;
            tdes.Padding = PaddingMode.PKCS7;

            ICryptoTransform cTransform = tdes.CreateDecryptor();

            byte[] resultArray =
            cTransform.TransformFinalBlock(Array_a_Descifrar, 0, Array_a_Descifrar.Length);

            tdes.Clear();
            //se regresa en forma de cadena
            return UTF8Encoding.UTF8.GetString(resultArray);
        }

    }
}
