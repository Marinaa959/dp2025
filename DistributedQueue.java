import java.util.Timer;
import java.util.Arrays;
import java.util.stream.Collectors;

public class DistributedQueue extends Process implements Lock {
    //p_k za pocetni proces, k = 0
    boolean object_present;
    int parent;
    boolean interested;
    int next;
    int obtained[];
    public DistributedQueue(Linker initComm, int k){
        super(initComm);
        if(myId == k) object_present = true;
        else object_present = false;
        parent = k;
        interested = false;
        next = -1;
        obtained = new int[N];
    }

    public synchronized void requestCS(){
        interested = true;
        if(!object_present){
            sendMsg(parent, "request", String.valueOf(myId));
            parent = myId;
            while(!object_present) myWait();
        }
    }

    public synchronized void releaseCS(){
        interested = false;
        if(next == -1){
            sendMsg(next, "object", objectString());
            object_present = false;
            next = -1;
        }
    }

    String objectString() {
        return Arrays.stream(obtained)
                    .mapToObj(String::valueOf)
                    .collect(Collectors.joining(","));
    }

    void read_object(String object){
        obtained = Arrays.stream(object.split(","))
                        .map(String::trim)
                        .mapToInt(Integer::parseInt)
                        .toArray();
        object_present = true;
    }

    void read_request(int k){
        if(parent != myId) sendMsg(parent, "request", String.valueOf(k));
        else if (interested) next = k;
        else{
            sendMsg(k, "object", objectString());
            object_present = false;
        }

        parent = k;
    }

    public synchronized void handleMsg(Msg m, int src, String tag){
        if(tag.equals("object")){
            read_object(m.getMessage());
            notify();
        }
        if(tag.equals("request")){
            read_request(Integer.parseInt(m.getMessage()));
        }
    }
}