FROM progaudi/dotnet:1.0.1-xenial

WORKDIR /app

# copy csproj and restore as distinct layers
COPY . .
RUN /app/scripts/build-netcore.sh