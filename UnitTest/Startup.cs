//using UnitTest.Models;
//using UnitTest.Repositories;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
//using UnitTest.Models;


namespace UnitTest
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }



        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            //services.AddScoped<IDWTableRepository, DWTableRepository>();
            //services.AddDbContext<DWTableContext>( o => o.UseSqlServer(Configuration.GetConnectionString("DTWttdpConnection")));

            //services.AddScoped<>
            services.AddControllers();
            services.AddSwaggerDocument(settings =>
            {
                settings.Title = "Unit Test for Datawarehouse";
            });
        }



        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env, ILoggerFactory loggerFactory)
        {
            loggerFactory.AddFile("Logs/asf-unittest-{Date}.txt");
            if (env.IsDevelopment())           
                app.UseDeveloperExceptionPage();           

            app.UseHttpsRedirection();

            app.UseRouting();

            app.UseAuthorization();

            app.UseOpenApi();
            app.UseSwaggerUi3();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
