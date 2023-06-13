#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 5000/tcp

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["dotnet.csproj", "dotnet/"]
RUN dotnet restore "dotnet/dotnet.csproj"
COPY . dotnet
WORKDIR "/src/dotnet"
RUN dotnet build "dotnet.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "dotnet.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "dotnet.dll"]