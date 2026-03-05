using System.Net.Http;
using System.Windows;
using Microsoft.Extensions.DependencyInjection;

namespace NdtBundleApp;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();

        var services = new ServiceCollection();
        services.AddWpfBlazorWebView();
        services.AddSingleton(new HttpClient());
        Resources.Add("services", services.BuildServiceProvider());
    }
}
