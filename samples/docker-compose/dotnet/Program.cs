using dotnet.Models;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using ProGaudi.MsgPack.Light;
using ProGaudi.Tarantool.Client.Model;
using ProGaudi.Tarantool.Client;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddMvc();

var msgPackContext = new MsgPackContext();
msgPackContext.GenerateAndRegisterArrayConverter<Dog>();

var clientOptions = new ClientOptions("operator:123123@tarantool1:3301", context: msgPackContext);

var box = new Box(clientOptions);
box.Connect().ConfigureAwait(false).GetAwaiter().GetResult();

builder.Services.AddSingleton(box);

var app = builder.Build();

// Configure the HTTP request pipeline.
app.UseStaticFiles();

app.UseRouting();

app.MapDefaultControllerRoute();

app.Run();
