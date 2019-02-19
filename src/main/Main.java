package main;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import join.join;

import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;

public class Main {
	public static void main(String[] args) throws Exception {
		
		//parse the sql request
		HashMap<String,String> manipulation = new HashMap<String,String>();
	    LinkedList<String> sql = new LinkedList<String>();
		for(int i = 0; i < args.length; i++){
	    	sql.add(args[i]);
	    	/*if(i == 1){
	    		String pattern = "count(.+\\s*.*)";
	    		Pattern p = Pattern.compile(pattern,Pattern.CASE_INSENSITIVE);
	    		Matcher m = p.matcher(args[1]);
	    		if(m.find()){
	    			String projection = args[1].substring(0, args[1].indexOf("count")-1);
	    			String aggregation = args[1].substring(args[1].indexOf("count"));
	    			System.out.println("project "+projection );
	    			System.out.println("aggregate  "+aggregation ); 
		    		if(projection != null){
		    			manipulation.put("project", projection);
		    		}
		    		if(aggregation != null){
		    			manipulation.put("aggregate", aggregation);
		    		}
	    		}
		    	else if(sql.contains("select")){
					manipulation.put("project", args[1]);
				}
	    	}*/
	    }
		if(sql.contains("select")){
			int index = sql.indexOf("select");
			System.out.println("select " + args[index+1]);
			manipulation.put("project", args[index+1]);
		}
		if(sql.contains("from")){
			int start = sql.indexOf("from")+1;
			System.out.println("from "+sql.get(start)+ "   "+ sql.get(start).getClass().getName());
			manipulation.put("from", sql.get(start));
		}
		if(sql.contains("where")){
			int index = sql.indexOf("where");
			String patternFilter = "[><=]\\d+";
			//String patternJoin = "=\\D+";
			Pattern PFilter = Pattern.compile(patternFilter,Pattern.CASE_INSENSITIVE);
			//Pattern PJoin = Pattern.compile(patternJoin,Pattern.CASE_INSENSITIVE);
			Matcher MFilter = PFilter.matcher(sql.get(index+1));
			String FilterKey = null;
			String JoinKey = null;
			if(MFilter.find()){	
			    FilterKey = sql.get(index+1);//args[index+1];
			}
			else{
				JoinKey = sql.get(index+1);//args[index+1];
				if(JoinKey != null)
					System.out.println("join "+JoinKey);
					manipulation.put("join", JoinKey);
			}
			index = index+2;
			while(index < sql.size() && (sql.get(index).toLowerCase().equals("and") || sql.get(index).toLowerCase().equals("or"))){
				MFilter = PFilter.matcher(sql.get(index+1));
				if(MFilter.find()){
					switch(sql.get(index)){
					case "and":
						if(FilterKey !=null)
							FilterKey = FilterKey + " and " + args[index+1];
						else
							FilterKey = args[index+1];
						break;
					case "or":
						if(FilterKey !=null)
							FilterKey = FilterKey + " or " + args[index+1];
						else
							FilterKey = args[index+1];
						break;
					default:
						break;
					}
				}
				index = index+2;
			}
			if(FilterKey != null){
				System.out.println("select "+ FilterKey);
				manipulation.put("select", FilterKey);
			}
	      }
		if(sql.contains("groupBy")){
			int index =sql.indexOf("groupBy");
			System.out.println("groupBy " + args[index+1]);
			manipulation.put("groupBy", args[index+1]);
		}
		if(sql.contains("count")){
			System.out.println("count");
			manipulation.put("count", null);
		}
		if(sql.contains("avg")){
			int index = sql.indexOf("avg");
			System.out.println("avg " + args[index+1]);
			manipulation.put("avg", args[index+1]);
		}
		if(sql.contains("max")){
			int index = sql.indexOf("max");
			System.out.println("max " + args[index+1]);
			manipulation.put("max", args[index+1]);
		}
		//HashMap<String,String> hashmap = new HashMap<String,String>();
		 join myjob=new join(manipulation);
		 myjob.goupbystart();
		
		
		
		/////
  }
}
