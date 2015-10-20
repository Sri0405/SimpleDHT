package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    static String received_all=null;
    static int count_received_all=0;

    private class node {

        String node_val;
        node predecessor;
        node succesor;

        node(String node_val) {
            this.node_val = node_val;
            predecessor = null;
            succesor = null;
        }

        void setnext(node entry) {
            this.succesor = entry;
        }

        void setprev(node entry) {
            this.predecessor = entry;
        }

    }

    private class CircularLinkedlist {
        node first;
        node end;
        int size;

        public CircularLinkedlist() {
            first = null;
            end = null;
            size =0;
        }

        public void insert(String val) {
            node entry = new node(val);
            if (first == null) {
                entry.setnext(entry);
                entry.setprev(entry);
                first = entry;
                end = first;
            } else {
                entry.setprev(end);
                end.setnext(entry);
                first.setprev(entry);
                entry.setnext(first);
                end = entry;
            }
            size=size+1;
        }

//        public int getsize()
//        {
//            return size;
//        }

//        public boolean isEmpty()
//        {
//            return getsize()==0;
//        }
    }

    static final String TAG = "Sridhar tag";
    static final int SERVER_PORT = 10000;
    static HashMap<Integer,String> hash = new HashMap<>();
    static HashSet<String> set = new HashSet<>();
    static int flag_val =0;
    int seq =0;

    static String tosend_all=null;
    static String res=null;

    static String array[]= new String[2];

    static boolean flag=false;
    static boolean flag_all =false;
    static Uri Muri;

    CircularLinkedlist chord_new = new CircularLinkedlist();

    private void getSortedOrder() {

        String arr[] = {"5562","5556","5554","5558","5560"};
        for(int i =0;i<5;i++)
        {
            if(set.contains(arr[i]))
            {
                chord_new.insert(arr[i]);
            }
        }

        flag_val=1;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.d(" present in delete",TAG);
        String key = selection;
        getContext().deleteFile(key);
        seq = seq - 1;
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

      if(set.size()==0 || set.size()==1)
      {
          String myport =getport();

          String key = values.get("key").toString();
          String val = values.get("value").toString();

          String arga[] ={key,val};
          insert_tosave(arga);
          Uri uri1=uri.withAppendedPath(uri,key);
          return uri1;
      }
        else
      {

          String key = values.get("key").toString();
          String val = values.get("value").toString();


          String myport = getport();


          String insert_here= getposition(key);

          String insert_her = Integer.toString(Integer.parseInt(insert_here)*2);
          if(insert_her.equals(myport))
          {
              Log.e("equals ",TAG);
              String args[]={key,val};
              insert_tosave(args);
          }
          else
          {
              String send =key+"%"+val+"%"+insert_her;
              new ClientTaskinsert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send, myport);
          }
      }
        return null;
    }

    private class ClientTaskinsert extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String msgReceived = msgs[0];

            String[] msgs_parts = msgReceived.split("%");

            try {
                String remotePort1= msgs_parts[2];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort1));

                String msgsend = msgs_parts[0]+"%"+msgs_parts[1]+"%"+"insert";
                PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                toServer.println(msgsend);
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }

    public String getposition(String keyval)  {

        if(flag_val == 0)
        {
            getSortedOrder();
        }

        node temp_first = chord_new.first;
        node temp_end = chord_new.end;
        String key = keyval;
        String insert_position = null;

        try {
            {

                node temp = chord_new.first;
                while (temp != chord_new.end) {

                    if (genHash(key).compareTo(genHash(temp.node_val)) > 0 && genHash(key).compareTo(genHash(temp.succesor.node_val)) <= 0) {

                        insert_position = temp.succesor.node_val;
                        Log.e((key),insert_position);
                        return insert_position;
                    }

                    temp = temp.succesor;

                }
            }
                  return temp_first.node_val;

        }catch (NoSuchAlgorithmException e)
        {

        }
        return null;
    }

    public Uri insert_tosave(String[] args) {

        Muri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        String key =args[0];
        String val =args[1];

        Uri uri1 = Muri.withAppendedPath(Muri, key);
        try {

            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(SimpleDhtActivity.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            Log.e(portStr+ "got to save insert_tosave in this avd"+key,TAG);
            hash.put(seq, key);
            FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            fos.write(val.getBytes());
            seq = seq + 1;
            fos.close();

        } catch (FileNotFoundException k) {

        } catch (IOException k) {

        }
        return uri1;
    }

    @Override
    public boolean onCreate() {

        // TODO Auto-generated method stub
        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
        }

        String myport = getport();
        String tosend = myport+"%"+ "nodejoin";
        Log.e("step 1 of sequence",TAG);

        set.add(Integer.toString(Integer.parseInt(getport())/2));

        if(!getport().equals("11108")) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, tosend, myport);
        }
            return true;

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            String msgReceived = msgs[0];

            try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            11108);
                    String msgToSend =msgReceived;
                    PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                    toServer.println(msgToSend);
                    socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            if (serverSocket != null) {
                while (true) {
                    BufferedReader input = null;
                    String line;
                    try {
                        Socket clientSocket = serverSocket.accept();
                        input = new BufferedReader(
                                new InputStreamReader(clientSocket.getInputStream()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    try {

                        line = input.readLine();

                        String[] line_parts = line.split("%");

                        if(line_parts[line_parts.length-1].equals("last"))
                        {
                            for(int i=1;i<line_parts.length-1;i++)
                            {
                                if(!set.contains(line_parts[i]))
                                {
                                    set.add(line_parts[i]);
                                }
                            }
                        }
                        if (line_parts.length == 3) {
                            if (line_parts[2].equals("insert")) {
                                String args[] = {line_parts[0], line_parts[1]};
                                insert_tosave(args);

                            }
                        }


                        if(line_parts.length ==2)
                        {
                         if(line_parts[1].equals("sendall"))
                             {
                             Log.e("Received req to send all",TAG);
                             String sendthis= line_parts[0]+"%"+line_parts[1];
                                 Log.e("calling another client query :",sendthis);
                             new ClientTaskQueryAllRep().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendthis, getport());
                             }
                        }

                        if (line_parts.length == 2) {

                            if (line_parts[1].equals("nodejoin")) {
                                String nod_val = Integer.toString(Integer.parseInt(line_parts[0]) / 2);
                                set.add(nod_val);
                                if(getport().equals("11108"))
                                {
                                    publishProgress(line);
                                }
                            }

                        }

                        if(line_parts.length==3)
                        {
                            //portekadanunchivachindo+"%"+key+"query"
                            if(line_parts[2].equals("query"))
                            {

                                {
                                  String sendthis= line_parts[0]+"%"+line_parts[1];
                                  new ClientTaskQueryRep().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendthis, getport());

                                }

                            }
                        }

                        if(line_parts[line_parts.length-1].equals("query_res_all"))
                        {
                            // reading string from all
                            Log.e(" received all queries query_res_all",Integer.toString(count_received_all));
                            count_received_all = count_received_all+1;
                            while(true)
                            {
                                if(count_received_all<=set.size())
                                {
                                    for(int i=1;i<line_parts.length-1;i++)
                                    {
                                        received_all= received_all +"%"+line_parts[i];
                                    }
                                }
                                break;
                            }
                        //    Log.e("recevied all is and flag_all is true",received_all);
                            if(count_received_all==set.size())
                            {
                                flag_all =true;
                            }

                        }

                        if(line_parts.length==3)
                        {//+matr_key+"%"+matr_val+"query_res"
                            if(line_parts[2].equals("query_res"))
                            {
                                Log.e("query reply received ",line);
                                flag =true;
                                array[0]=line_parts[0];
                                array[1]=line_parts[1];
                            }
                        }

                    } catch (IOException e) {
                        e.printStackTrace();

                    }
                }
            }
            return null;
        }


        protected void onProgressUpdate(String... strings) {

            String myport = getport();
            String strReceived = strings[0].trim();
            String parts[] = strReceived.split("%");

            if(parts.length ==2)
            {
                 String nodetosend =parts[0];
                 String arr[] = {"5562","5556","5554","5558","5560"};
                 String sendStr = nodetosend;

                 for(int i =0;i<5;i++)
                 {
                    if(set.contains(arr[i]))
                    {
                        sendStr= sendStr+"%"+arr[i];
                    }
                 }
                Log.e("Step 5" +sendStr,TAG);
                new ClientTaskSendBackSize().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendStr, getport());
            }

            if (parts.length == 4) {
                new ClientTaskProgesss().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strReceived, myport);
            }

        }
    }

    private class ClientTaskQueryAllRep extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String strReceived =msgs[0];
            String parts[]=strReceived.split("%");

            try {
                //line_parts[0]+"%"line_parts[1]"
                String result_to_send = query_local_string_all();
         //     Log.e("Query reply sending backing to port  "+parts[0],result_to_send);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(parts[0]));
                String msgToSend = result_to_send+"%"+"query_res_all";
                PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                toServer.println(msgToSend);
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    private class ClientTaskQueryRep extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String strReceived =msgs[0];
            String parts[]=strReceived.split("%");

            try {
                //line_parts[0]+"%"line_parts[1]"
                String result_to_send = query_local_string(parts[1]);
                Log.e("Query reply sending to port "+parts[0],result_to_send);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(parts[0]));
                String msgToSend = result_to_send+"%"+"query_res";
                PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                toServer.println(msgToSend);
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    private class ClientTaskProgesss extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String strReceived =msgs[0];
            String parts[] = strReceived.split("%");
            String key = parts[0];
            String val = parts[1];
            String position =parts[2];
            String flag =parts[3];

            try {

                Integer remotePort1 = (Integer.parseInt(position)*2);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                (remotePort1));

                String msgToSend = key+"%"+val+"%"+flag;
                PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                toServer.println(msgToSend);
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }

    private class ClientTaskSendBackSize extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String strReceived =msgs[0];

            String ports[]= {REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
            try {
                for (int i =0;i < 5;i++)
                {
                    String remotePort1= ports[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort1));
                    Log.e("step 6",TAG);
                    String msgToSend = strReceived+"%"+"last";
                    PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                    toServer.println(msgToSend);
                    socket.close();
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

//    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        String key = selection;

        if (set.size() == 1 || set.size() == 0) {
            if(selection.contains("@") || selection.contains("*"))
                return query_getall();
            else
                return query_local(selection);
        } else if (selection.contains("@")) {
            return query_getall();
        } else if (selection.contains("*") && set.size()!=1) {

            String sendall = "sendall";
            Log.d("got a query to *","sending to clienttaskqueryall");
            new ClientTaskqueryall().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, sendall, getport());

            while (true) {
                if (flag_all == true) {

                    String arr[] = {"key", "value"};
                    MatrixCursor matrixCursor = new MatrixCursor(arr);
                    Log.e("Flag_all is true ", "return matrix object");
                    String parts_all[] = received_all.split("%");
                    Log.e("Flag_all is true",Integer.toString(parts_all.length));
                    Log.e("this is last",received_all);

                    for (int i = 1; i <= (set.size())*5; i=i+1) {

                       String keyval = parts_all[i];
                       String keyvalp[]=keyval.split("#");

                       String rows[] = {keyvalp[0], keyvalp[1]};
                       Log.e(rows[0],rows[1]);
                       Log.e("inside final matrix returning loop ",Integer.toString(i));
                       matrixCursor.addRow(rows);

                    }
                    flag_all = false;
         //           received_all=null;
                    return matrixCursor;
                }
            }
        } else {
            String querpos = getposition(key);
            String querypor = Integer.toString(Integer.parseInt(querpos) * 2);

            if (getport().equals(querypor)) {
                return query_local(key);
            } else {
                String que = querypor + "%" + key + "%" + "query";
                new ClientTaskquery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, que, getport());

                while (true) {
                    if (flag == true) {
                        String arr[] = {"key", "value"};
                        MatrixCursor matrixCursor = new MatrixCursor(arr);
                        String rows[] = {array[0], array[1]};
                        matrixCursor.addRow(rows);
                        flag = false;
                        return matrixCursor;
                    }
                }

            }

        }

    }


    private class ClientTaskqueryall extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String msgReceived = msgs[0];

            node temp =chord_new.first;

            for(int i=0 ; i < set.size() ; i++)
            {
                try {
                    int port = Integer.parseInt(temp.node_val)*2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            port);
                    //msgreceived = sendall

                    Log.e("forwarding query_all req",Integer.toString(port));
                    //ekadanuncho + send all
                    String msgToSend = getport()+"%"+msgReceived;
                    Log.e("sending message to all",msgToSend);

                    PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                    toServer.println(msgToSend);
                    socket.close();
                    temp=temp.succesor;
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }

            return null;

        }
    }

    private class ClientTaskquery extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String msgReceived = msgs[0];
            String msgparts[]=msgReceived.split("%");
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgparts[0]));
                String msgToSend = getport()+"%"+msgparts[1]+"%"+msgparts[2];
                PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);
                toServer.println(msgToSend);
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;

        }
    }

    public MatrixCursor query_getall()
    {
        String arr[] = {"key", "value"};
        MatrixCursor matrixCursor = new MatrixCursor(arr);
        try {

            for (int i = 0; i < seq; i++) {
                String key_loop = hash.get(i);
                FileInputStream fis = getContext().openFileInput(key_loop);
                StringBuffer fileContent = new StringBuffer("");

                byte[] buffer = new byte[1024];
                int n = fis.read(buffer);
                fileContent.append(new String(buffer, 0, n));
                String val = fileContent.toString();
                String rows[] = {key_loop, val};
                matrixCursor.addRow(rows);
            }
            matrixCursor.close();
            return matrixCursor;
        }catch (Exception e)
        {

        }
        return matrixCursor;

    }


    public String query_local_string_all()
    {
        String tosendall=null;
        try {
            for (int i = 0; i < seq; i++) {
                String key_loop = hash.get(i);

                FileInputStream fis = getContext().openFileInput(key_loop);
                StringBuffer fileContent = new StringBuffer("");

                byte[] buffer = new byte[1024];
                int n = fis.read(buffer);
                fileContent.append(new String(buffer, 0, n));
                String val = fileContent.toString();
                tosendall = tosendall +"%" +key_loop+"#"+val;
            }

        }catch (Exception e)
        {
        }
       return tosendall;
    }

    public MatrixCursor query_local(String key)
    {
        MatrixCursor matrixCursor = null;
        String arr[] = {"key", "value"};

        try {
            matrixCursor = new MatrixCursor(arr);
            FileInputStream fis = getContext().openFileInput(key);
            StringBuffer fileContent = new StringBuffer("");

            byte[] buffer = new byte[1024];
            int n = fis.read(buffer);

            fileContent.append(new String(buffer, 0, n));

            String val = fileContent.toString();
            String rows[] = {key, val};
            matrixCursor = new MatrixCursor(arr);
            matrixCursor.addRow(rows);
            matrixCursor.close();
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
        }
        return matrixCursor;
    }


    public String query_local_string(String key)
    {
        String result=null;
        try {
            FileInputStream fis = getContext().openFileInput(key);
            StringBuffer fileContent = new StringBuffer("");

            byte[] buffer = new byte[1024];
            int n = fis.read(buffer);

            fileContent.append(new String(buffer, 0, n));

            String val = fileContent.toString();
            result = key+"%"+val;
            Log.e("Query Problem here",result);
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
        }
        return result;
    }

    public String getport()
    {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(SimpleDhtActivity.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myport = String.valueOf((Integer.parseInt(portStr) * 2));
        return myport;
    }

        @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}