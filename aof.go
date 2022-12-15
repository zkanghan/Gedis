package main

import (
	"bufio"
	"errors"
	"log"
	"os"
	"strconv"
)

func rewriteAppendOnlyFileBackground() error {
	//TODO: check if any coroutines are already being rewritten
	//TODO: Parent-child coroutine communication
	if server.aofRewriteState != -1 {
		return errors.New("already rewriting")
	}
	return nil
}

//This function also called when the save command is executed
func rewriteAppendOnlyFile(filename string) error {
	now := GetTimeMs()
	//create temp file
	tempFile := "temp-rewriteAOF.aof"

	fp, err := os.Create(tempFile)
	if err != nil {
		log.Printf("create temp file error when aof rewriting: %v \n", err)
		return err
	}
	fp, err = os.OpenFile(tempFile, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Printf("open file in rewriteAppendOnlyFile error: %v \n", err)
		return err
	}
	aof := NewRioWithFile(fp)
	di := NewDictSafeIterator(server.db.data)
	for e := di.DictNext(); e != nil; e = di.DictNext() {
		key := e.Key
		o := e.Val

		//don't save if it has expired
		expireTime := getExpire(key)
		if expireTime != -1 && expireTime < now {
			continue
		}

		switch o.Type_ {
		case STR:
			setCmd := []byte("*3\r\n$3\r\nset\r\n")
			if err = aof.Write(setCmd, len(setCmd)); err != nil {
				goto wErr
			}
			if err = aof.WriteBulkString(key.StrVal()); err != nil {
				goto wErr
			}
			if err = aof.WriteBulkString(o.StrVal()); err != nil {
				goto wErr
			}
		}
		// save the expiry time
		if expireTime != -1 {
			cmd := []byte("*3\r\n$9\r\npexpireat\r\n")
			if aof.Write(cmd, len(cmd)) != nil {
				goto wErr
			}
			if aof.WriteBulkString(key.StrVal()) != nil {
				goto wErr
			}
			if aof.WriteBulkInt64(expireTime) != nil {
				goto wErr
			}
		}
	}
	ReleaseIterator(di)

	//make sure data will not remain on the OS's output buffers
	if err = aof.file.fp.Sync(); err != nil {
		log.Printf("sync disk error when aof rewriting: %v \n", err)
		goto wErr
	}
	if err = aof.file.fp.Close(); err != nil {
		log.Printf("close file error when aof rewriting: %v \n", err)
		goto wErr
	}

	//Use RENAME to make sure the DB file is changed atomically only if the generate DB file is ok.
	if err = os.Rename(tempFile, filename); err != nil {
		log.Printf("moving temp append only file on the final destination error: %v", err)
		_ = os.Remove(tempFile)
		return err
	}

	log.Printf("SYNC append only file rewrite done\n")
	return nil

wErr:
	aof.file.fp.Close()
	_ = os.Remove(tempFile)
	if di != nil {
		ReleaseIterator(di)
	}
	return err
}

func loadAppendOnlyFile(filename string) error {
	//create a fake client
	fakeClient := NewClient(0)
	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		log.Printf("open append only file for reading error: %v \n", err)
		return err
	}
	sn := bufio.NewScanner(f)
	for true {
		if !sn.Scan() {
			if sn.Err() != nil {
				goto rErr
			}
			break
		}

		if sn.Bytes()[0] != '*' {
			goto fmtErr
		}
		argc := bytes2int(sn.Bytes()[1:])
		if argc < 1 {
			goto fmtErr
		}

		for i := 0; i < argc; i++ {
			//there will be 'args' number of argument be read in expectations. if not we consider it format error
			if !sn.Scan() {
				goto fmtErr
			}

			if sn.Bytes()[0] != '$' {
				goto fmtErr
			}

			strLen := bytes2int(sn.Bytes()[1:])

			if !sn.Scan() {
				goto fmtErr
			}

			argStr := string(sn.Bytes())
			if len(argStr) != strLen {
				goto fmtErr
			}
			fakeClient.args = append(fakeClient.args, NewObject(STR, argStr))
		}

		//command lookup
		cmd := lookUpCommand(fakeClient.args[0].StrVal())
		if cmd == nil {
			log.Printf("unknown command %s readind append only file \n", fakeClient.args[0].StrVal())
			return errors.New("unknown command")
		}

		cmd.proc(fakeClient)
		//reset fake client
		fakeClient.args = make([]*GObj, 0)
	}

	// this point can only be reached when EOF is reached without errors.
	_ = f.Close()
	return nil

rErr:
	log.Printf("read append only file error: %v \n", err)
	return err

fmtErr:
	log.Printf("bad file format reading the append only file \n")
	return errors.New("invalid aof file format")
}

//the main process calls this function when the child thread completes the AOF rewrite.
func bgRewriteDoneHandler() {
	//TODO
}

func bytes2int(b []byte) int {
	num, _ := strconv.Atoi(string(b))
	return num
}
