# syntax=docker/dockerfile:1.7

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY src/TextMediation.Worker/TextMediation.Worker.csproj src/TextMediation.Worker/
RUN dotnet restore src/TextMediation.Worker/TextMediation.Worker.csproj

COPY . .

FROM build AS publish
RUN dotnet publish src/TextMediation.Worker/TextMediation.Worker.csproj \
    -c Release \
    -o /app/publish \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=publish /app/publish .

ENV DOTNET_EnableDiagnostics=0 \
    DOTNET_gcServer=1 \
    DOTNET_TieredPGO=1

ENTRYPOINT ["dotnet", "TextMediation.Worker.dll"]