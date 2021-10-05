using System;
using System.Collections.Generic;
using System.Text;

namespace Tools
{
    /// <summary>
    /// Clase que Administra la libreria de Matematicas propia de la compania
    /// </summary>  
    public class SkyMath
    {
        /// <summary> 
        /// Operacion que permite sumar n elementos 
        /// </summary>
        /// <param name="elemento">Elementos que se desean suma</param>
        /// <returns>La suma de todos los elementos</returns>
        public static double Sumar(double[] elemento)
        {
            double suma = 0;
            foreach (var item in elemento)
            {
                suma += item;
            }
            return suma;
        }

        /// <summary>
        /// Funcion que eleva una base a su exponenete
        /// </summary>
        /// <param name="base2">La base</param>
        /// <param name="exponente">El exponenete</param>
        /// <returns>Retorna un <see cref="System.Double"/> como resultado de la operacion exponencial</returns>
        /// <exception cref="System.ArgumentException">
        /// <p>Base : La 'Base' no menor o igual que cero</p>
        /// <p>Exponente : El 'Exponente' no puede ser menor o igual que cero</p>
        /// </exception>       
        public static double Pow(int base2, int exponente)
        {
            if (base2 <= 0) throw new ArgumentException("Base", "La 'Base' no menor o igual que cero");
            if (exponente <= 0) throw new ArgumentException("Exponente", "El 'Exponente' no puede ser menor o igual que cero");

            double resultado = 1;
            int expoAux = exponente;

            if (exponente != 0)
            {// Si es distinto de cero            
                if (exponente < 0)
                {// si es menor que cero               
                    exponente = exponente * -1;
                    for (int a = 1; a <= exponente; a++)
                    {
                        resultado = resultado * base2;
                    }
                    if (resultado != 0)
                    {
                        resultado = 1 / (resultado);
                        resultado = Math.Round(resultado + 1);
                    }
                    else
                    {
                        resultado = 1 / (1);
                    }
                }
                else
                {
                    for (int a = 1; a <= exponente; a++)
                    {
                        resultado = resultado * base2;
                    }
                }
            }
            else
            {
                resultado = 1;
            }
            return resultado;
        }


        /// <summary>
        /// Metodo de la division que evita el error de la division entre cero
        /// </summary>
        /// <param name="Numerador">Numerador de la division</param>
        /// <param name="Denominador">Denominador de la division</param>
        /// <returns>Retorna un <see cref="System.Double"/> como resultado de la division</returns>        
        /// <exception cref="System.ArgumentException">
        /// <p>Numerador : El Numerador no menor o igual que cero</p>
        /// <p>Denominador : El Denominado no puede ser menor o igual que cero</p>
        /// </exception>        
        public static double Division(Double Numerador, Double Denominador)
        {
            if (Numerador <= 0) throw new ArgumentException("Numerador", "El Numerador no menor o igual que cero");
            if (Denominador <= 0) throw new ArgumentException("Denominador", "El Denominado no puede ser menor o igual que cero");

            try
            {
                return (Numerador / Denominador);
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary> 
        /// Operacion que permite hacer n multiplicaciones 
        /// </summary>
        /// <param name="elemento">Elementos que se desean multiplicar</param>
        /// <returns>Retorna un <see cref="System.Double"/>con la suma de todos los elementos</returns>       
        public static double Multiplicacion(double[] elemento)
        {
            double suma = 0;
            foreach (var item in elemento)
            {
                suma *= item;
            }
            return suma;
        }
    }
}
