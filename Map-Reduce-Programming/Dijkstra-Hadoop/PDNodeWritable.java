package ParallelDijkstra;
import java.util.*;

public class PDNodeWritable {

    public int nodeID;
    public int prev;
    public int distance;
    public HashMap<Integer,Integer> AdjacentList;
    
    public PDNodeWritable() {
        this.nodeID = -1;
        this.prev = -1;
        this.distance = 1000000000;
        this.AdjacentList = new HashMap<Integer,Integer>();
    }

    public void setNodeID(int value){
        nodeID = value;
    }
    
    public void setDistance(int value){
        distance = value;
    }
    
    public void setPrev(int value){
        prev = value;
    }
    
    public int getNodeID(){
        return nodeID;
    }

    public int getDistance(){
        return distance;
    }
    
    public String getPrev(){
        if(prev == -1)
            return "nil";
        else
            return String.valueOf(prev);
    }
    
    public String toString(){
        
        StringBuffer sb = new StringBuffer();
        sb.append(distance).append(" ").append(prev).append(" ");
        for(Integer d : AdjacentList.keySet())
            sb.append(d.toString()).append("-").append(AdjacentList.get(d).toString()).append(" ");
        return sb.toString().trim();
        
    }
    
    public void fromString(String value){
        
        StringTokenizer stk = new StringTokenizer(value);
        int AdjacentNode = 0;
        int Weight = 0;
        
        if(stk.hasMoreElements())
            nodeID = Integer.parseInt(stk.nextToken());       
        if(stk.hasMoreElements())
            distance = Integer.parseInt(stk.nextToken());
        if(stk.hasMoreElements())
            prev = Integer.parseInt(stk.nextToken());
        while(stk.hasMoreElements()){
            StringTokenizer stk2 = new StringTokenizer(stk.nextToken(),"-");
            if(stk2.hasMoreElements())
                AdjacentNode = Integer.parseInt(stk2.nextToken());
            if(stk2.hasMoreElements())
                Weight = Integer.parseInt(stk2.nextToken());
            AdjacentList.put(AdjacentNode, Weight);
        }
        
    }
    
    public String getList(){
        
        StringBuffer sb = new StringBuffer();
        for(Integer d : AdjacentList.keySet())
            sb.append(d.toString()).append("-").append(AdjacentList.get(d).toString()).append(" ");
        return sb.toString().trim();
        
    }

}
