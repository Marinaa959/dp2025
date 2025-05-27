import java.util.Timer;
import java.util.Arrays;
import java.util.stream.Collectors;
public class CompleteObject extends Process implements Lock {
    boolean haveObject;
    int[] obtained;    // OBJECT
    boolean wantCS = false;
    int[] request_by;
    public CompleteObject(Linker initComm, int coordinator) {
        super(initComm);
        haveObject = (myId == coordinator);
        request_by = new int[N];
        obtained = new int[N];
    }
    public synchronized void initiate() {
        // if (haveObject) sendObject();
    }
    public synchronized void requestCS() {
        wantCS = true;
        if (!haveObject) {
            request_by[myId]++;
            broadcastMsg("request", myId);
            while (!haveObject) myWait();
        }
    }
    public synchronized void releaseCS() {
        wantCS = false;
        obtained[myId] = request_by[myId];
        for (int k = myId + 1; k < N; k++) {
            if (request_by[k] > obtained[k]) {
                sendObject(k);
                return;
            }
        }
        for (int k = 0; k < myId; k++) {
            if (request_by[k] > obtained[k]) {
                sendObject(k);
                return;
            }
        }
    }
    void sendObject(int destId) {
        if (haveObject && !wantCS) {
            sendMsg(destId, "object", objectString());
            haveObject = false;
        }
    }
    String objectString() {
        return Arrays.stream(obtained)
                    .mapToObj(String::valueOf)
                    .collect(Collectors.joining(","));
    }
    void readObject(String object) {
        obtained = Arrays.stream(object.split(","))
                        .map(String::trim)
                        .mapToInt(Integer::parseInt)
                        .toArray();
    }
    public synchronized void handleMsg(Msg m, int src, String tag) {
        if (tag.equals("object")) {
            haveObject = true;
            readObject(m.getMessage());
            notify();
        }
        if (tag.equals("request")) {
            request_by[src]++;
            sendObject(src);
        }
    }
}
