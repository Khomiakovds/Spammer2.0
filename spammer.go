package main

import (
	"fmt"
	"sort"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	in := make(chan interface{})
	out := make(chan interface{})
	wg := &sync.WaitGroup{}

	for _, FuncOfCmd := range cmds{
		wg.Add(1)
		go func(in , out chan interface{},FuncOfCmd func(in,out chan interface{})){
			defer func(){
				wg.Done()
				close(out)
			}()

			FuncOfCmd(in,out)

		}(in,out,FuncOfCmd)
		in = out
		out = make(chan interface{})

	}
	wg.Wait()

}

func SelectUsers(in, out chan interface{}) {
	UniqueIds := make(map[uint64]struct{})
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	for currentEmail := range in {
		wg.Add(1)
		go func(currentEmail string){
			defer wg.Done()
			currentUser := GetUser(currentEmail)
			if сheckUniqueIdUsers(UniqueIds,currentUser.ID,mu){
				out <- currentUser
			}

		}(currentEmail.(string))

	}
	wg.Wait()

}

func сheckUniqueIdUsers(unique map[uint64]struct{},newId uint64,mu *sync.Mutex)bool{
	mu.Lock()
	defer mu.Unlock()
	if _, exitst:=unique[newId];!exitst{
		unique[newId] = struct{}{}
		return true
	}
	return false
}

func SelectMessages(in, out chan interface{}) {
UserBatch := make([]User, 0)
wg := &sync.WaitGroup{}
for currentUser := range in {
	UserBatch = append(UserBatch, currentUser.(User))
	if len(UserBatch) >= GetMessagesMaxUsersBatch{
		SelectMessagesOfUsersBatch(UserBatch[0:2],out,wg)
		UserBatch = UserBatch[2:]
	}


}
if len(UserBatch) >= 0{
	SelectMessagesOfUsersBatch(UserBatch,out,wg)

}

wg.Wait()
}

func SelectMessagesOfUsersBatch(UserBatch []User,out chan interface{},wg *sync.WaitGroup){
	wg.Add(1)
	go func(){
		defer wg.Done()
		MessOfUserBatchID,err := GetMessages(UserBatch...)
		if err==nil {
			for _, currntMess := range MessOfUserBatchID{
				out <- currntMess

			}
		}
	}()

}



func CheckSpam(in, out chan interface{}) {
	antiBrut := make(chan struct{}, HasSpamMaxAsyncRequests)
	wg := &sync.WaitGroup{}
	for currentMess := range in {
		wg.Add(1)
		go func(antiBrut chan struct{},currentMess MsgID){
			antiBrut <- struct{}{}
			defer  func(){
				<-antiBrut
				wg.Done()
			}()
			spamInfo,err := HasSpam(currentMess)

			if err == nil {
				out <-MsgData{currentMess,spamInfo}
			}


			
			


		}(antiBrut,currentMess.(MsgID))
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}){
	var spamInfo MsgPull
	for currentResult := range in{
		spamInfo = append(spamInfo, currentResult.(MsgData))
	}

	sort.Sort(spamInfo)

	for _,currentSpamInfo := range spamInfo{
		currentSpamString := fmt.Sprintf("%v %d" , currentSpamInfo.HasSpam , currentSpamInfo.ID)
		out <- currentSpamString

	}

}


type MsgPull []MsgData

func (MsgD MsgPull) Len()int{
	return len(MsgD)
}

func (MsgD MsgPull) Less(i,j int)bool{
	if MsgD[i].HasSpam != MsgD[j].HasSpam{
		return MsgD[i].HasSpam
	}else{
		return MsgD[i].ID < MsgD[j].ID
	}
}

func (MsgD MsgPull) Swap(i,j int){
	MsgD[i],MsgD[j] = MsgD[j],MsgD[i]
}
