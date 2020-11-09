//! Signal Messenger client for terminal

mod app;
mod config;
mod signal;
mod ui;
mod util;

use app::{App, Event};

use crossterm::{
    event::{
        DisableMouseCapture, EnableMouseCapture, Event as CEvent, EventStream, KeyCode,
        KeyModifiers,
    },
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use structopt::StructOpt;
use tokio::stream::StreamExt;
use tokio::sync::Notify;
use tui::{backend::CrosstermBackend, Terminal};

//writeln append debugging to file is commented out.
//debug.txt file must be manually (re)created before use.
//use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;

#[derive(Debug, StructOpt)]
struct Args {
    /// Enable logging to `gurg.log` in the current working directory.
    #[structopt(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::from_args();

    let mut app = App::try_new(args.verbose)?;

    enable_raw_mode()?;
    let _raw_mode_guard = scopeguard::guard((), |_| {
        disable_raw_mode().unwrap();
    });

    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;

    let events = Arc::new(Notify::new());
    let events2 = events.clone();

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    tokio::spawn({
        let mut tx = tx.clone();
        async move {
            let mut reader = EventStream::new().fuse();
            while let Some(event) = reader.next().await {
                //let mut debug = OpenOptions::new().append(true).open("debug.txt").unwrap();
                //writeln!(debug, "(A)Await for event stream about to run...");
                events2.notified().await;
                //writeln!(debug, "(B)Await for event stream just ran...");
                match event {
                    Ok(CEvent::Key(key)) => tx.send(Event::Input(key)).await.unwrap(),
                    Ok(CEvent::Resize(_, _)) => tx.send(Event::Resize).await.unwrap(),
                    _ => (),
                }
            }
        }
    });

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let signal_client = signal::SignalClient::from_config(app.config.clone());
    tokio::spawn(async move { signal_client.stream_messages(tx).await });

    terminal.clear()?;

    loop {
        events.notify();
        terminal.draw(|f| ui::draw(f, &mut app))?;
        match rx.recv().await {
            Some(Event::Input(event)) => match event.code {
                KeyCode::Char('c') if event.modifiers.contains(KeyModifiers::CONTROL) => {
                    break;
                }
                KeyCode::Left => app.on_left(),
                KeyCode::Up => app.on_up(),
                KeyCode::Right => app.on_right(),
                KeyCode::Down => app.on_down(),
                KeyCode::Char('a') if event.modifiers.contains(KeyModifiers::CONTROL) => {
                    let events3 = events.clone();
                    tokio::spawn(async move {
                        //let mut debug = OpenOptions::new().append(true).open("debug.txt").unwrap();
                        let open_file: String;
                        //writeln!(debug, "File dialog about to run...");
                        // TODO use the open_file_dialog_multi function
                        match tinyfiledialogs::open_file_dialog("Attach file", "", None) {
                            Some(file) => open_file = file,
                            None => open_file = "null".to_string(),
                        }
                        println!("Open file {:?}", open_file);
                        //writeln!(debug, "(2)Notify of events about to run...");
                        events3.notify();
                    });
                    //let mut debug = OpenOptions::new().append(true).open("debug.txt").unwrap();
                    //writeln!(debug, "(1)Await of events about to run...")?;
                    events.notified().await;
                    //writeln!(debug, "(3)Events just woke up from await!")?;
                    let size = terminal.size().unwrap();
                    terminal.resize(size)?;
                }
                code => app.on_key(code),
            },
            Some(Event::Message { payload, message }) => {
                app.on_message(message, payload).await;
            }
            Some(Event::Resize) => {
                // will just redraw the app
            }
            None => {
                break;
            }
        }
        if app.should_quit {
            break;
        }
    }

    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )
    .unwrap();
    terminal.show_cursor().unwrap();

    Ok(())
}
