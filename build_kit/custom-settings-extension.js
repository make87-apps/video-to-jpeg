const vscode = require('vscode');

function activate(context) {
  vscode.workspace.openTextDocument('/home/state/code/app/main.py').then(document => {
    vscode.window.showTextDocument(document);

    const terminal = vscode.window.createTerminal('Setup Terminal');
    terminal.show(true); // Show the terminal and make it visible
    terminal.sendText('source /home/state/venv/bin/activate'); // Send the pip install command to the terminal
    terminal.sendText('pip install -e .'); // Send the pip install command to the terminal
  });
}

exports.activate = activate;
