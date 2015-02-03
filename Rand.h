#include<time.inl>
#include <profileapi.h>
#include <Windows.h>

class TUInt64
{
public:
	unsigned __int64 Val;
	TUInt64(const unsigned __int64& MsVal, const unsigned __int64& LsVal) : Val(0)
	{
		Val = (((unsigned __int64)MsVal) << 32) | ((unsigned __int64)LsVal);
	}
};


class TRnd{
public:
	static const int RndSeed = 0;
	double GetUniDev(){ return GetNextSeed() / double(m); }
	TRnd(const int& _Seed = 1, const int& Steps = 0)
	{
		PutSeed(_Seed); Move(Steps);
	}
	void TRnd::PutSeed(const int& _Seed)
	{
		if (_Seed == 0){
			//Seed=int(time(NULL));
			Seed = abs(int(GetPerfTimerTicks()));
		}
		else {
			Seed = _Seed;
			//Seed=abs(_Seed*100000)+1;
		}
	}

	void TRnd::Move(const int& Steps){
		for (int StepN = 0; StepN<Steps; StepN++){ GetNextSeed(); }
	}

	unsigned __int64 GetPerfTimerTicks()
	{
		unsigned int MsVal; unsigned int LsVal;
		LARGE_INTEGER LargeInt;
		if (QueryPerformanceCounter(&LargeInt))
		{
			MsVal = LargeInt.u.HighPart;
			LsVal = LargeInt.u.LowPart;
		}
		else {
			MsVal = 0;
			LsVal = int(time(NULL));
		}
		TUInt64 UInt64(MsVal, LsVal);
		return UInt64.Val;
	}
private:
	static const int a = 16807, m = 2147483647, q = 127773, r = 2836;
	int Seed;
	int GetNextSeed(){
		if ((Seed = a*(Seed%q) - r*(Seed / q)) > 0){ return Seed; }
		else { return Seed += m; }
	}

};