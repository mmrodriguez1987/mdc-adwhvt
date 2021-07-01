using UnitTest.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using UnitTest.Dictionary;
using System.Reflection;
using System.Data.Entity;

namespace UnitTest.Repositories
{
    public class DWTableRepository
    {
        /*private DWTableContext _context;
       private readonly DWDictionary _dictionary;

       public DWTableRepository(DWTableContext context)
       {
           _context = context;      
       }




               public List<DWTableContext> GetRowsCount(string tableName)
               {
                   using (var _context = new DWTableRepository())
                   {
                       return "";
                   }

                   _context.Database.OpenConnection();

                   throw new NotImplementedException();
               }

       public async Task<DWTable> Get(string tableName)
       {
           var entityName = Assembly.GetExecutingAssembly().GetTypes().FirstOrDefault(t => t.Name == tableName);
           var query = _context.Set(entityName);



           throw new NotImplementedException();
       }

       public int GetRowCount(string tablename)
       {
           throw new NotImplementedException();
       }

       public List<DWTableContext> GetRowsCount(string tableName)
       {
           throw new NotImplementedException();
       }*/



    }
}
