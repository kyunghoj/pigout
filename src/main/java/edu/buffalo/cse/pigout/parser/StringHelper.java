package edu.buffalo.cse.pigout.parser;

public class StringHelper {
	public static String escapeInputString(String input){
		if(input == null){
			return "";
		}
		StringBuilder sb = new StringBuilder();
		int inputLength = input.length();

		//if(input.startsWith('')){}
		for(int i = 0; i < inputLength; i++){
			char ch = input.charAt(i);
			if(Character.isUnicodeIdentifierPart(input.charAt(0))){
				String argStr = StringHelper.getUnicodeString(input);
				sb.append(argStr);
				continue;
			}
			switch(ch){
			case '\t':
				sb.append("\\t");
				break;
			case '\\':
                sb.append('\\');
                break;
            case '\'':
                sb.append("\\'");
                break;
            case '\r':
                sb.append("\\r");
                break;
            case '\f':
                sb.append("\\f");
                break;
            case '\n':
                sb.append("\\n");
                break;
            case '\b':
                sb.append("\\b");
                break;
				default:
					sb.append(ch);
					break;
			}
		}
		return sb.toString();
	}
	
	public static String getUnicodeString(String str_code){       
		String hexCode = Integer.toHexString(str_code.codePointAt(0)).toUpperCase();        
	    String hexCodeWithAllLeadingZeros = "0000" + hexCode;
	    String hexCodeWithLeadingZeros = hexCodeWithAllLeadingZeros
	    .substring(hexCodeWithAllLeadingZeros.length() - 4);
	    hexCodeWithLeadingZeros = "\\u" + hexCodeWithLeadingZeros;
	    System.out.println("Unicode " + hexCodeWithLeadingZeros);
	    return hexCodeWithLeadingZeros;  
	}
}
