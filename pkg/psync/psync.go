package psync

import (
	"context"
	"fmt"
)

type Psync struct {
	ctx       context.Context
	cancel    context.CancelFunc
	src, dest *redis
}

func New(srcAddr, destAddr string) *Psync {
	ctx, cancel := context.WithCancel(context.Background())
	return &Psync{
		ctx:    ctx,
		cancel: cancel,
		src:    newRedis(srcAddr),
		dest:   newRedis(destAddr),
	}
}

func (p *Psync) Go() {
	fmt.Println("starting Sync")
	defer p.cleanup()
	err := p.src.connect(p.ctx)
	if err != nil {
		panic(err)
	}
	err = p.dest.connect(p.ctx)
	if err != nil {
		panic(err)
	}
	p.src.writer.ping()
	_, err = p.src.reader.readLine()
	if err != nil {
		panic("failed to read pong")
	}
	err = p.sync()
	if err != nil {
		panic(err)
	}
	go p.log(p.dest)
}

func (p *Psync) cleanup() {
	p.cancel()
	p.src.close()
	p.dest.close()
}

func (p *Psync) sync() error {
	p.dest.writer.flushall()
	p.src.writer.capa()
	_, err := p.src.reader.readLine()
	if err != nil {
		return fmt.Errorf("failed to send capa :%w", err)
	}
	p.src.writer.sync()
	err = p.src.reader.readRDB()
	if err != nil {
		return fmt.Errorf("failed to sync RDB data :%w", err)
	}
	p.repl()
	return nil
}

func (p *Psync) repl() error {
	fmt.Println("replicating commands...")
	for {
		select {
		case <-p.ctx.Done():
			fmt.Println("shutting down repl")
			return nil
		default:
			b, err := p.src.reader.readCommand()
			if err != nil {
				if p.ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("failed to read command :%w", err)
			}
			//fmt.Printf("%s", b)
			p.dest.writer.raw(b)
		}
	}
}

func (p *Psync) log(r *redis) {
	for {
		select {
		case <-p.ctx.Done():
			fmt.Println("shutting down log for redis:", r)
		default:
			str, err := r.reader.readLine()
			if err != nil {
				if p.ctx.Err() != nil {
					return
				}
				panic(fmt.Errorf("failed to read line :%w", err))
			}
			fmt.Printf("%s", str)
		}
	}
}
