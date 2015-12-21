
public class CheckIfDouble {
	public boolean isNumeric(String str)
	{ try
		{ double d = Double.parseDouble(str); 
			} 
	catch(NumberFormatException nfe)
	{ 	
		return false; 
		} 
	
	return true; }
}
