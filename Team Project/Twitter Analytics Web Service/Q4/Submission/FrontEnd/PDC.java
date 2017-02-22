package CloudComputing.TeamServer;

public class PDC {
	private static final String X = "26362928081002303087713271852220986810381868672656104434480496829300700800354435353790711445136516282";

	public int keyGen(String y){
		int xLength = X.length();
		if(y == null){
			return -1;
		}
		int yLength = y.length();
		if(yLength > 1){
			y = y.substring(yLength - 2);
		}
		int newYLength = y.length();
		String[] xArray = X.split("");
		String[] yArray = y.split("");
		int result = 0;
		int[] zArray = new int[newYLength];
		for(int i = 0; i < newYLength; i++){
			zArray[i] = Integer.parseInt(yArray[i]);
		}
		int currentX = 0;
		int currentZ = 0;
		for(int i = yLength - newYLength; i <= xLength-newYLength; i++){
			for(int j = i; j < i+newYLength; j++){
				currentX = Integer.parseInt(xArray[j]);
				currentZ = zArray[j-i];
				zArray[j - i] = (currentZ + currentX);
			}
		}
		
		if(newYLength > 1){
			result += (zArray[0] % 10)*10 + zArray[1]%10;
		}else{
			result += (zArray[0])%10;
		}
		return result;
	}
	
	public char reverseCaesarify(int miniKey, char charValue ){
			char result;
			if(charValue - miniKey < 65){
				result = (char)(26 + charValue - miniKey);
			}else{
				result = (char)( charValue - miniKey);
			}
		return result;
	}
	
	public String spiralize(String x, int z){
		//Get miniKey

		int miniKey = z % 25 + 1;
		char[] xArray = x.toCharArray();
		
		char[] result = new char[xArray.length];
		int length = result.length;
		int level = (int)(1 + Math.sqrt(1 + 8 * length)) / 2;
		int i = 0;
		int step = 0;
		
		int resultPosition = 0;
		int bottomCount = level - 1;
		int bCount = 0;
		
		int downCount = level - 1;
		int dCount = 1;
		
		int upCount = level - 2;
		int uCount = 0;
		
		int upStep = -level-1;
		result[resultPosition] = reverseCaesarify(miniKey, xArray[i]);
		if(x.length() == 1){
			return String.valueOf(result);
		}
		boolean atBottom = false;
		boolean goUp = false;
		boolean goDown = true;
		while(resultPosition < length){
			if(goDown){
				step++;
				i += step;
				dCount++;
				if(dCount == downCount){ // touch bottom
					goDown = false;
					atBottom = true;
					downCount -= 3;
					dCount = 0;
					bCount++;
				}
			}else if(atBottom){
				step = 1;
				i += step;
				bCount++;
				if(bCount == bottomCount){ //touch the end of bottom
					atBottom = false;
					goUp = true;
					bottomCount -= 3;
					bCount = 0;
					uCount++;
					step = upStep + 1;
					upStep++;
				}
			}else if(goUp){
				step++;
				i += step;
				uCount++;
				if(uCount == upCount){
					goUp = false;
					goDown = true;
					upCount -= 3;
					uCount = 0;
					step = -step-2;
				}
			}
			// put the value into result array
			resultPosition++;
			result[resultPosition] = reverseCaesarify(miniKey, xArray[i]);
			if(resultPosition == result.length - 1){
				break;
			}
		}
		
		return String.valueOf(result);
	}
}
