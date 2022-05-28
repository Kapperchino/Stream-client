package stream.client.app.cli;

import java.util.ArrayList;
import java.util.List;


/**
 * This class enumerates all the commands enqueued by FileStore state machine.
 */
public final class ClientCli {
    private ClientCli() {
    }

    public static List<SubCommandBase> getSubCommands() {
        List<SubCommandBase> commands = new ArrayList<>();
        commands.add(new Publish());
        return commands;
    }
}
