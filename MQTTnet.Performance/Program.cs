﻿using MQTTnet;
using MQTTnet.Protocol;
using MQTTnet.Server;
using System.Threading.Channels;

const int MsgPerSecond = 20000;
const int SubscriberClients = 20000;
const MqttQualityOfServiceLevel ServiceQuality = MqttQualityOfServiceLevel.AtLeastOnce;

MqttServerFactory serverFactory = new();

MqttServerOptions serverOptions = new MqttServerOptionsBuilder()
    .WithDefaultEndpoint()
    .Build();

var mqttServer = serverFactory.CreateMqttServer(serverOptions);
await mqttServer.StartAsync();

MqttClientFactory factory = new();
IMqttClient publisherClient = factory.CreateMqttClient();

var publisherOptions = new MqttClientOptionsBuilder()
            .WithTcpServer("localhost")
            .WithClientId("publisher")
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(10))
            .WithCleanSession()
            .Build();

// Connect the publisher client
await publisherClient.ConnectAsync(publisherOptions);

Console.WriteLine("Connecting the subscriber clients...");

TestClient mainReceiverClient = new TestClient(factory.CreateMqttClient(), "main", ServiceQuality);
await mainReceiverClient.StartAsync();

// Connect the other dummy clients
for(int i = 0; i < SubscriberClients - 1; i++)
{
    TestClient subscriberClient = new TestClient(factory.CreateMqttClient(), $"{i}", ServiceQuality);
    await subscriberClient.StartAsync();
}

// Create a channel for publishing messages
var channel = Channel.CreateUnbounded<(string topic, string payload)>();

// Task to continuously read from the channel and publish messages
_ = Task.Run(async () =>
{
    await foreach (var (topic, payload) in channel.Reader.ReadAllAsync())
    {
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(payload)
            .WithQualityOfServiceLevel(ServiceQuality)
            .Build();

        await publisherClient.PublishAsync(message);
    }
});

// Timer to send messages to the channel every second
var timer = new System.Timers.Timer(1000);
timer.Elapsed += (sender, e) =>
{
    Console.WriteLine($"Received Msg Count: {mainReceiverClient.ReceivedMsgCount}");
    Console.WriteLine($"PUBLISHER CHANNEL SIZE: {channel.Reader.Count}");
    for (int i = 0; i < MsgPerSecond; i++)
    {
        channel.Writer.TryWrite(("client/main", "Hello World"));
    }
};
timer.Start();

Console.ReadLine();

class TestClient
{
    private readonly IMqttClient _client;
    private readonly MqttClientOptions _options;
    private readonly MqttQualityOfServiceLevel _serviceQuality;

    public int ReceivedMsgCount { get; private set; } = 0;

    public TestClient(IMqttClient client, string clientId, MqttQualityOfServiceLevel serviceQuality)
    {
        _client = client;
        _serviceQuality = serviceQuality;
        _options = new MqttClientOptionsBuilder()
            .WithTcpServer("localhost")
            .WithClientId(clientId)
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(10))
            .WithCleanSession()
            .Build();
    }

    public async Task StartAsync()
    {
        _client.ApplicationMessageReceivedAsync += e =>
        {
            ReceivedMsgCount++;
            return Task.CompletedTask;
        };
        await _client.ConnectAsync(_options);
        await _client.SubscribeAsync(new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter($"client/{_options.ClientId}", _serviceQuality)
            .Build());
    }
}