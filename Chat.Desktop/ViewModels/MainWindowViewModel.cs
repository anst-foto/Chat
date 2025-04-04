using System.Collections.ObjectModel;
using System.Reactive;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

using ReactiveUI;
using ReactiveUI.Fody.Helpers;

namespace Chat.Desktop.ViewModels;

public class MainWindowViewModel : ViewModelBase
{
    [Reactive] public string? UserName { get; set; }
    [Reactive] public string? Message { get; set; }
    [Reactive] public bool IsEnabled { get; set; } = true;

    public ObservableCollection<string> Messages { get; } = [];
    
    public ReactiveCommand<Unit, Unit> SaveUserName { get; }
    public ReactiveCommand<Unit, Unit> SendMessage { get; }

    public MainWindowViewModel()
    {
        var canExecuteSendMessage = this.WhenAnyValue(
            vm => vm.Message,
            vm => vm.UserName, 
            (m, u) => !string.IsNullOrWhiteSpace(m) &&
                      !string.IsNullOrWhiteSpace(u));
        var canExecuteSaveUserName = this.WhenAnyValue(
            vm => vm.UserName,
            vm => vm.IsEnabled, 
            (u, e) => !string.IsNullOrWhiteSpace(u) && e);
        
        const string channelName = "chat-stream";

        var streamSystem = StreamSystem.Create(new StreamSystemConfig()).Result;
        streamSystem.CreateStream(new StreamSpec(channelName)
        {
            MaxLengthBytes = 5_000_000_000
        });
        
        var producer = Producer.Create(new ProducerConfig(streamSystem, channelName)).Result;
        
        SaveUserName = ReactiveCommand.Create(() => { IsEnabled = !IsEnabled; }, canExecuteSaveUserName);
        SendMessage = ReactiveCommand.CreateFromTask(async () =>
        {
            var message = $"{UserName}: {Message}";
            await producer.Send(new Message(Encoding.UTF8.GetBytes(message)));
        }, canExecuteSendMessage);
        
        Consumer.Create(new ConsumerConfig(streamSystem, channelName)
        {
            OffsetSpec = new OffsetTypeFirst(),
    
            MessageHandler = async (_, _, _, message) =>
            {
                var messageText = Encoding.UTF8.GetString(message.Data.Contents);
                Messages.Add(messageText);
                await Task.CompletedTask;
            }
        });
    }
}